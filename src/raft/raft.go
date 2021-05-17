package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// Heartbeat provides implementation for multichannel heartbeat.
type Heartbeat struct {
	C        chan int
	waiter   sync.Cond
	killer   []chan int
	resetter []chan int
}

func (timer *Heartbeat) kill() {
	for _, ch := range timer.killer {
		ch <- 1
	}
}
func (timer *Heartbeat) timerTask(id int, interval time.Duration) {
	t := time.After(interval)
TIMER_LOOP:
	for {
		select {
		case <-t:
			select {
			case timer.C <- id:
				t = time.After(interval)
				continue TIMER_LOOP
			case <-timer.killer[id]:
				return
			case <-timer.resetter[id]:
				t = time.After(interval)
				continue TIMER_LOOP
			}
		case <-timer.killer[id]:
			return
		case <-timer.resetter[id]:
			t = time.After(interval)
			continue TIMER_LOOP
		}
	}
}
func makeHeartbeat(size int, interval time.Duration) *Heartbeat {
	killer := make([]chan int, size)
	resetter := make([]chan int, size)
	for i := 0; i < size; i++ {
		killer[i] = make(chan int, 1)
		resetter[i] = make(chan int, 1)
	}
	timer := Heartbeat{
		C:        make(chan int),
		waiter:   sync.Cond{},
		killer:   killer,
		resetter: resetter,
	}
	for i := 0; i < size; i++ {
		go timer.timerTask(i, interval)
	}

	return &timer
}
func (timer *Heartbeat) resetTimer(id int) bool {
	select {
	case timer.resetter[id] <- 0:
		return true
	default:
		return false
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Term   int
	RealID int
	Data   interface{}
}
type RaftPersistentInfo struct {
	CurrentTerm                int64
	VotedFor                   int
	Log                        []Log
	SnapshotLastIncludedIndex  int
	SnapshotLastIncludedTerm   int
	SnapshotLastIncludedRealID int
	//LogID       int
}

func (info *RaftPersistentInfo) getCurrentTerm() int {
	return int(atomic.LoadInt64(&info.CurrentTerm))
}

func (info *RaftPersistentInfo) setCurrentTerm(term int) {
	atomic.StoreInt64(&info.CurrentTerm, int64(term))
}

type RaftVolatileInfoAll struct {
	commitIndex int
	lastApplied int
}
type RaftVolatileInfoLeader struct {
	nextIndex  []int
	matchIndex []int
}

//
// A Go object implementing a single Raft peer.
//

const (
	Follower  int32 = 0
	Candidate int32 = 1
	Leader    int32 = 2
)

type EventAppendEntriesReq struct {
	req      *AppendEntriesArgs
	res      *AppendEntriesReply
	doneChan chan int
}

func (req *EventAppendEntriesReq) done() {
	req.doneChan <- 0
}
func (req *EventAppendEntriesReq) abort() {
	req.doneChan <- 1
}

type EventAppendEntriesRes struct {
	id            int
	res           *AppendEntriesReply
	source        *AppendEntriesArgs
	oldMatchIndex int
	oldNextIndex  int
}
type EventRequestVoteReq struct {
	req      *RequestVoteArgs
	res      *RequestVoteReply
	doneChan chan int
}

func (req *EventRequestVoteReq) done() {
	req.doneChan <- 0
}
func (req *EventRequestVoteReq) abort() {
	req.doneChan <- 1
}

type EventRequestVoteRes struct {
	id     int
	res    *RequestVoteReply
	source *RequestVoteArgs
}

type EventAppendEvent struct {
	command  interface{}
	index    *int
	term     *int
	isLeader *bool
	doneChan chan int
}

func (req *EventAppendEvent) done() {
	req.doneChan <- 0
}
func (req *EventAppendEvent) abort() {
	req.doneChan <- 1
}

type EventCondSnapshotInstall struct {
	lastIncludedTerm  int
	lastIncludedIndex int
	snapshot          []byte
	success           *bool
	doneChan          chan int
}

func (req *EventCondSnapshotInstall) done() {
	req.doneChan <- 0
}
func (req *EventCondSnapshotInstall) abort() {
	req.doneChan <- 1
}

type CSIKey struct {
	LastIncludedRealID int
}
type CSIValue struct {
	lastIncludedIndex int
	counter           int
}

type Raft struct {
	//mu        sync.Mutex          // Mutexes are for cowards.
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistent        RaftPersistentInfo
	currentSnapshot   []byte
	volatile          RaftVolatileInfoAll
	volatileLeader    *RaftVolatileInfoLeader
	status            int32
	snapshotRealIDMap map[CSIKey]CSIValue
	eventQueue        chan interface{}
	killer            chan int
	applyCh           chan ApplyMsg

	asyncApplyCh chan *ApplyMsg
	rwmuKilled   *sync.RWMutex
}

// Raft does not allow us to bring RealID in CondInstallSnapshot.
// Simulate it, since ApplyMsg and CondInstallSnapshot are in pairs.
func (rf *Raft) getSnapshotID(realid int) int {
	k := CSIKey{LastIncludedRealID: realid}
	v, ok := rf.snapshotRealIDMap[k]
	if !ok {
		panic("No id found")
	}
	v.counter--
	if v.counter == 0 {
		delete(rf.snapshotRealIDMap, k)
	} else {
		rf.snapshotRealIDMap[k] = v
	}

	return v.lastIncludedIndex
}

func (rf *Raft) setSnapshotID(realid int, id int) {
	k := CSIKey{LastIncludedRealID: realid}
	v, ok := rf.snapshotRealIDMap[k]
	if ok {
		if id != v.lastIncludedIndex {
			panic("Bad Snapshot ID")
		}
		v.counter++
		rf.snapshotRealIDMap[k] = v
	} else {
		v.lastIncludedIndex = id
		v.counter = 1
		rf.snapshotRealIDMap[k] = v
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.persistent.getCurrentTerm()
	isleader = rf.getStatus() == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistent)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistWithSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistent)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.currentSnapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.persistent = RaftPersistentInfo{
			CurrentTerm:                0,
			VotedFor:                   -1,
			Log:                        make([]Log, 0),
			SnapshotLastIncludedIndex:  -1,
			SnapshotLastIncludedTerm:   -1,
			SnapshotLastIncludedRealID: 0,
			//LogID:       1,
		}
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.persistent) != nil {
		panic("Bad state")
	}
	rf.volatile.commitIndex = rf.persistent.SnapshotLastIncludedIndex
	rf.volatile.lastApplied = rf.volatile.commitIndex
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2A, 2B).
	rf.rwmuKilled.RLock()
	if rf.killed() {
		rf.rwmuKilled.RUnlock()
		return false

	}
	c := make(chan int, 1)
	success := false
	rf.eventQueue <- EventCondSnapshotInstall{
		lastIncludedTerm:  lastIncludedTerm,
		lastIncludedIndex: lastIncludedIndex,
		snapshot:          snapshot,
		success:           &success,
		doneChan:          c,
	}
	rf.rwmuKilled.RUnlock()
	r := <-c
	if r != 0 {
		return false
	}

	return success
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	c := make(chan int, 1)
	ev := EventTakeSnapshot{
		realID:   index,
		snapshot: snapshot,
		doneChan: c,
	}
	rf.rwmuKilled.RLocker().Lock()
	if !rf.killed() {
		rf.eventQueue <- ev
	}
	rf.rwmuKilled.RLocker().Unlock()
	<-c
	// Your code here (2D).

}

func (req *EventTakeSnapshot) done() {
	req.doneChan <- 0
}

func (req *EventTakeSnapshot) abort() {
	req.doneChan <- 1
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.rwmuKilled.RLock()
	if rf.killed() {
		reply.Term = rf.persistent.getCurrentTerm()
		reply.VoteGranted = false
		rf.rwmuKilled.RUnlock()
		return
	}
	c := make(chan int, 1)
	rf.eventQueue <- EventRequestVoteReq{
		req:      args,
		res:      reply,
		doneChan: c,
	}
	rf.rwmuKilled.RUnlock()
	r := <-c
	if r != 0 {
		reply.Term = rf.persistent.getCurrentTerm()
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rwmuKilled.RLock()
	if rf.killed() {
		reply.Term = rf.persistent.getCurrentTerm()
		reply.Success = false
		rf.rwmuKilled.RUnlock()
		return
	}
	c := make(chan int, 1)
	rf.eventQueue <- EventAppendEntriesReq{
		req:      args,
		res:      reply,
		doneChan: c,
	}
	rf.rwmuKilled.RUnlock()
	r := <-c

	if r != 0 {
		reply.Term = rf.persistent.getCurrentTerm()
		reply.Success = false
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
	// We magically combines InstallSnapshots and AppendEntries into one.
	// Leader will send both Data and current Entries.
	// This saves a call.
	IsInstallSnapshot     bool
	LastIncludedIndex     int
	LastIncludedTerm      int
	LastIncludedRealIndex int
	SnapshotData          []byte
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	// True for enabling ConflictTerm and ConflictIndex the first index for ConflictTerm. False for ConflictIndex being the last index of the log.
	IsConflict    bool
	ConflictIndex int
	ConflictTerm  int
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Async version.
// Just send. The return value is left in event queue.
func (rf *Raft) broadcastRequestVoteAsync(args *RequestVoteArgs) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			j := i
			go (func() {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(j, args, reply)
				if ok {
					rf.rwmuKilled.RLock()
					if !rf.killed() {
						rf.eventQueue <- EventRequestVoteRes{id: j, res: reply, source: args}
					}
					rf.rwmuKilled.RUnlock()
				}
			})()
		}
	}

}

func (rf *Raft) getInLogIndex(index int) int {
	return index - rf.persistent.SnapshotLastIncludedIndex - 1
}

func (rf *Raft) sendAppendEntriesAsync(i int) {
	rf.tracef("Sending AE to %d", i)
	if i != rf.me {
		if rf.volatileLeader.nextIndex[i] <= rf.persistent.SnapshotLastIncludedIndex {
			// Send snapshot
			args := &AppendEntriesArgs{
				Term:                  rf.persistent.getCurrentTerm(),
				LeaderID:              rf.me,
				IsInstallSnapshot:     true,
				LastIncludedIndex:     rf.persistent.SnapshotLastIncludedIndex,
				LastIncludedTerm:      rf.persistent.SnapshotLastIncludedTerm,
				LastIncludedRealIndex: rf.persistent.SnapshotLastIncludedRealID,
				SnapshotData:          rf.currentSnapshot,
			}
			j := i
			go (func() {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(j, args, reply)
				if ok {
					rf.rwmuKilled.RLock()
					if !rf.killed() {
						rf.eventQueue <- EventAppendEntriesRes{id: j, res: reply, source: args}
					}
					rf.rwmuKilled.RUnlock()
				}
			})()
		} else {
			plt := rf.persistent.SnapshotLastIncludedTerm
			if rf.getInLogIndex(rf.volatileLeader.nextIndex[i]) > 0 {
				plt = rf.persistent.Log[rf.getInLogIndex(rf.volatileLeader.nextIndex[i]-1)].Term
			}
			newSlice := append([]Log(nil), rf.persistent.Log[rf.getInLogIndex(rf.volatileLeader.nextIndex[i]):]...)
			args := &AppendEntriesArgs{
				Term:         rf.persistent.getCurrentTerm(),
				LeaderID:     rf.me,
				PrevLogIndex: rf.volatileLeader.nextIndex[i] - 1,
				PrevLogTerm:  plt,
				Entries:      newSlice,
				LeaderCommit: rf.volatile.commitIndex,
			}
			oldMatchIndex := rf.volatileLeader.matchIndex[i]
			oldNextIndex := rf.volatileLeader.nextIndex[i]
			j := i
			go (func() {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(j, args, reply)
				if ok {
					rf.rwmuKilled.RLock()
					if !rf.killed() {
						rf.eventQueue <- EventAppendEntriesRes{id: j, res: reply, source: args, oldMatchIndex: oldMatchIndex, oldNextIndex: oldNextIndex}
					}
					rf.rwmuKilled.RUnlock()
				}
			})()
		}

	}
}
func (rf *Raft) broadcastAppendEntriesAsync() {
	rf.tracef("Sending heartbeat")
	for i := 0; i < len(rf.peers); i++ {
		rf.sendAppendEntriesAsync(i)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.rwmuKilled.RLock()
	if rf.killed() {
		return 0, rf.persistent.getCurrentTerm(), false
	}
	c := make(chan int)
	index := 0
	term := 0
	isLeader := false
	request := EventAppendEvent{
		command:  command,
		doneChan: c,
		index:    &index,
		term:     &term,
		isLeader: &isLeader,
	}
	rf.eventQueue <- request // Send to main thread.
	rf.rwmuKilled.RUnlock()
	r := <-c
	if r != 0 {
		return 0, rf.persistent.getCurrentTerm(), false
	}

	return index, term, isLeader
}

func (rf *Raft) setStatus(status int32) {
	atomic.StoreInt32(&rf.status, status)
}
func (rf *Raft) getStatus() int32 {
	return atomic.LoadInt32(&rf.status)
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	rf.rwmuKilled.Lock()
	if rf.killed() {
		return // Killing twice is not allowed.
	}
	atomic.StoreInt32(&rf.dead, 1)
	rf.rwmuKilled.Unlock()
	rf.killer <- 0
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Heartbeat 110ms.
func heartbeatTimeout() <-chan time.Time {
	return time.After(110 * time.Millisecond)
}

// Election 1000-1200 ms, randomly.
func electionTimeout() (<-chan time.Time, time.Duration) {
	dur := (1000 + (time.Duration)(rand.Int31n(200)))
	return time.After(dur * time.Millisecond), dur
}

func (rf *Raft) becomeFollower(newTerm int, votedFor int) {
	rf.persistent.setCurrentTerm(newTerm)
	rf.persistent.VotedFor = votedFor
	rf.volatileLeader = nil
	rf.setStatus(Follower)
	rf.persist()
}

// Returns: Index, Term, RealID
func (rf *Raft) getLastLogInfo() (int, int, int) {
	if len(rf.persistent.Log) > 0 {
		x := &rf.persistent.Log[len(rf.persistent.Log)-1]
		return rf.persistent.SnapshotLastIncludedIndex + len(rf.persistent.Log), x.Term, x.RealID
	} else {
		return rf.persistent.SnapshotLastIncludedIndex, rf.persistent.SnapshotLastIncludedTerm, rf.persistent.SnapshotLastIncludedRealID
	}
}

type EventTakeSnapshot struct {
	realID   int
	snapshot []byte
	doneChan chan int
}

func (rf *Raft) getAbsoluteIndex(id int) int {
	return id + rf.persistent.SnapshotLastIncludedIndex + 1
}
func (rf *Raft) compactLogs(realID int, snapshot []byte) {
	if realID <= rf.persistent.SnapshotLastIncludedRealID {
		return // No we don't want to snapshot old commits.
	}
	for i := 0; i < len(rf.persistent.Log); i++ {
		if rf.persistent.Log[i].RealID == realID {
			if rf.getAbsoluteIndex(i) > rf.volatile.commitIndex {
				panic("Trying to compact uncommitted log %d. Bad.")
			}
			rf.tracef("Compacting till log %d (realid=%d, %v)", rf.getAbsoluteIndex(i), rf.persistent.Log[i].RealID, rf.persistent.Log[i].Term)
			// Start compaction.
			rf.persistent.SnapshotLastIncludedRealID = rf.persistent.Log[i].RealID
			rf.persistent.SnapshotLastIncludedTerm = rf.persistent.Log[i].Term
			rf.persistent.SnapshotLastIncludedIndex = rf.getAbsoluteIndex(i)
			// reduce log.
			rf.persistent.Log = rf.persistent.Log[i+1:]
			// store snapshot.
			rf.currentSnapshot = snapshot
			rf.persistWithSnapshot()
			return
		}
	}
	panic("No log with RealID found, not to say commited log.")
}

/// The first argument being success or not, the second argument being the first index for the term.
func (rf *Raft) tryAppendLogs(req *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{
		Success: false,
		Term:    rf.persistent.getCurrentTerm(),
	}
	if req.IsInstallSnapshot {
		rf.tracef("Installing snapshot until log %d.", req.LastIncludedIndex)
		reply.Success = true
		if req.LastIncludedIndex <= rf.volatile.commitIndex {
			// outdated snapshot.
			return reply
		}
		rf.setSnapshotID(req.LastIncludedRealIndex, req.LastIncludedIndex)
		msg := &ApplyMsg{
			SnapshotValid: true,
			SnapshotTerm:  req.LastIncludedTerm,
			SnapshotIndex: req.LastIncludedRealIndex,
			Snapshot:      req.SnapshotData,
		}
		rf.asyncApplyCh <- msg
		/*
			rf.volatile.commitIndex = req.LastIncludedIndex
			rf.volatile.lastApplied = req.LastIncludedIndex
			rf.persistent.SnapshotLastIncludedIndex = req.LastIncludedIndex
			rf.persistent.SnapshotLastIncludedRealID = req.LastIncludedRealIndex
			rf.persistent.SnapshotLastIncludedTerm = req.LastIncludedTerm
			rf.persistent.Log = make([]Log, 0)
			rf.currentSnapshot = clone(req.SnapshotData)
			rf.persistWithSnapshot()
		*/
		return reply
	} else {
		l, t, _ := rf.getLastLogInfo()
		rf.tracef("Appending remote log from %d (prev=%d, t=%d) to current logs (last=%d, t=%d)", req.LeaderID, req.PrevLogIndex, req.PrevLogTerm, l, t)
		if l < req.PrevLogIndex {
			rf.tracef("Failed since there is no such log.")
			reply.IsConflict = false
			reply.ConflictIndex = l
			return reply
		}
		// Only care about entries after commitIndex.
		prevIndex := req.PrevLogIndex
		prevLogTerm := req.PrevLogTerm
		newEntries := req.Entries
		inLogPrevIndex := rf.getInLogIndex(prevIndex)
		if prevIndex <= rf.volatile.commitIndex {
			if rf.volatile.commitIndex-prevIndex >= len(newEntries) {
				rf.tracef("All committed. Abort.")
				reply.Success = true
				return reply
			}
			newEntries = newEntries[rf.volatile.commitIndex-prevIndex:]
			prevIndex = rf.volatile.commitIndex
			prevLogTerm = rf.persistent.SnapshotLastIncludedTerm
			inLogPrevIndex = rf.getInLogIndex(prevIndex)
			if inLogPrevIndex >= 0 {
				prevLogTerm = rf.persistent.Log[inLogPrevIndex].Term
			}

		}

		if inLogPrevIndex != -1 && rf.persistent.Log[inLogPrevIndex].Term != prevLogTerm {
			rf.tracef("Failed due to log term mismatch.")
			reply.IsConflict = true
			reply.ConflictTerm = rf.persistent.Log[inLogPrevIndex].Term
			coni := req.PrevLogIndex
			lastSatisfying := req.PrevLogIndex
			for i := inLogPrevIndex; i >= 0 && rf.persistent.Log[i].Term == reply.ConflictTerm; i-- {
				lastSatisfying = coni
				coni--
			}
			reply.ConflictIndex = lastSatisfying
			return reply
		}

		logs := rf.persistent.Log
		for id, log := range newEntries {
			absid := id + inLogPrevIndex + 1
			if absid >= len(logs) {
				logs = append(logs, log)
			} else {
				if logs[absid].Term != log.Term {
					logs = append(logs[0:absid], log)
				}
				// otherwise, skip.
			}
		}
		//if req.PrevLogIndex+1+len(req.Entries) > len(rf.persistent.Log) {
		//temp := req.PrevLogIndex + 1 + len(req.Entries) - len(rf.persistent.Log)
		//rf.persistent.Log = append(rf.persistent.Log[0:req.PrevLogIndex+1], req.Entries...)
		rf.persistent.Log = logs
		if TraceVerbose {
			rf.tracef("Appended %d elements successfully.", len(req.Entries))
		}
		//} else {
		// No allocation. Just copy.
		//	l := copy(rf.persistent.Log[req.PrevLogIndex+1:], req.Entries)
		//	if l != len(req.Entries) {
		//		rf.tracef("Some copy is wrong")
		//	}
		//	if TraceVerbose {
		//		rf.tracef("Overwriting %d elements successfully.", len(req.Entries))
		//	}
		//}

		rf.persist()
		rf.commitLog(req.LeaderCommit)
		reply.Success = true
		return reply
	}

}

// Election property.
func (rf *Raft) electionProperty(req *RequestVoteArgs) bool {
	t1 := req.LastLogTerm
	i2, t2, _ := rf.getLastLogInfo()

	if t1 > t2 {
		return true
	}
	if t1 < t2 {
		return false
	}

	return req.LastLogIndex >= i2
}

const (
	TraceEnabled bool = true
	TraceVerbose bool = true
)

func (rf *Raft) getLastRealID() int {
	_, _, rid := rf.getLastLogInfo()
	return rid
}
func (rf *Raft) computeCommittedIndex() int {
	/*if len(rf.peers) == 1 {
		return rf.volatile.commitIndex
	}*/
	tmp := make([]int, len(rf.volatileLeader.matchIndex))
	threshold := (len(rf.peers) + 1) / 2
	copy(tmp, rf.volatileLeader.matchIndex)
	lastIndex, _, _ := rf.getLastLogInfo()
	tmp[rf.me] = lastIndex
	sort.Ints(tmp)
	// Why is it this?
	// Suppose there are 5 peers, 0 to 4, sorted.
	// The commited index should be stored in matchIndex[2], where 2 = 5-3, and 3 the threshold.
	// Thus 2, 3 and 4 must have the value committed.
	return tmp[len(rf.peers)-threshold]

}
func (rf *Raft) commitLog(index int) {
	if index > rf.volatile.commitIndex {
		inLogIndex := rf.getInLogIndex(index)
		inLogCommitIndex := rf.getInLogIndex(rf.volatile.commitIndex) // >=-1
		if rf.persistent.Log[inLogIndex].Term == rf.persistent.getCurrentTerm() {
			for i := inLogCommitIndex + 1; i <= inLogIndex; i++ {
				id := rf.persistent.Log[i].RealID
				switch cmd := rf.persistent.Log[i].Data.(type) {
				case CommandNop:
					if TraceVerbose {
						rf.tracef("Committing %d NOP for term %d.", rf.getAbsoluteIndex(i), cmd.Term)
					}
				default:
					rf.asyncApplyCh <- &ApplyMsg{
						CommandValid: true,
						CommandIndex: id, /* Awww seems we have messed up indices. But no worry.*/
						Command:      cmd,
					}
					if TraceVerbose {
						rf.tracef("Commit %d(realid=%d, %v)", rf.getAbsoluteIndex(i), id, cmd)
					}
				}

			}

			rf.tracef("Commiting logs from %d to %d.", rf.volatile.commitIndex+1, index)
			rf.volatile.commitIndex = index
			rf.volatile.lastApplied = rf.volatile.commitIndex // TODO: separate apply
		}

	}
}

func (rf *Raft) leaderUpdateMatchIndex() {
	rf.commitLog(rf.computeCommittedIndex())
}
func (rf *Raft) followerUpdateCommitIndex(leaderCommit int) {
	rf.commitLog(leaderCommit)
}
func (rf *Raft) tracef(msg string, args ...interface{}) {
	if TraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[Raft][%d-%d-%d-%d][#%d Term %d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), rf.me, rf.persistent.getCurrentTerm(), m)
	}
}

func (rf *Raft) initializeLeader() {
	rf.volatileLeader = &RaftVolatileInfoLeader{
		nextIndex:  make([]int, len(rf.peers)),
		matchIndex: make([]int, len(rf.peers)),
	}

	index, _, _ := rf.getLastLogInfo()
	for i := 0; i < len(rf.peers); i++ {
		rf.volatileLeader.nextIndex[i] = index + 1
		rf.volatileLeader.matchIndex[i] = -1
	}
}

func (rf *Raft) tryAbortEvent(ev *interface{}) {
	switch ev := (*ev).(type) {
	case EventAppendEntriesReq:
		rf.tracef("Cleaning an AE event. Feeling good.")
		ev.abort()
	case EventRequestVoteReq:
		rf.tracef("Cleaning a RV event. Feeling good.")
		ev.abort()
	case EventAppendEvent:
		rf.tracef("Rejecting a log. Feeling good.")
		ev.abort()
	case EventCondSnapshotInstall:
		rf.tracef("Cleaning a CSI event. Feeling good.")
		ev.abort()
	case EventTakeSnapshot:
		rf.tracef("Aborting a snapshot request. Feeling good.")
		ev.abort()
	}
}

func (rf *Raft) condSnapshotInstall(lastIncludedTerm int, lastIncludedRealID int, snapshot []byte) bool {
	lastIncludedIndex := rf.getSnapshotID(lastIncludedRealID)
	if rf.volatile.commitIndex >= lastIncludedIndex {
		return false
	}
	// lastIncludedIndex>commitIndex>=old lastIncludedIndex
	// Now check if (lastIncludedIndex, lastIncludedTerm) matches one in current log.
	// There are only two possibilities: match or unmatch.
	// When unmatch, simply drop all logs.
	// When match, simple keep all logs.
	inLogIndex := rf.getInLogIndex(lastIncludedIndex)
	if inLogIndex >= len(rf.persistent.Log) || rf.persistent.Log[inLogIndex].Term != lastIncludedTerm {
		rf.persistent.Log = make([]Log, 0)
	} else {
		rf.persistent.Log = rf.persistent.Log[inLogIndex+1:]
	}
	rf.tracef("Installing new snapshot id=%d term=%d rid=%d", lastIncludedIndex, lastIncludedTerm, lastIncludedRealID)
	rf.persistent.SnapshotLastIncludedIndex = lastIncludedIndex
	rf.persistent.SnapshotLastIncludedTerm = lastIncludedTerm
	rf.persistent.SnapshotLastIncludedRealID = lastIncludedRealID
	rf.volatile.commitIndex = lastIncludedIndex
	rf.volatile.lastApplied = rf.volatile.commitIndex
	rf.currentSnapshot = snapshot
	rf.persistWithSnapshot()
	return true
}

type CommandNop struct {
	Term int
}

func (rf *Raft) termLeader() {
	rf.initializeLeader()
	// Append nop.
	rf.persistent.Log = append(rf.persistent.Log, Log{RealID: rf.getLastRealID(), Term: rf.persistent.getCurrentTerm(), Data: CommandNop{Term: rf.persistent.getCurrentTerm()}})
	rf.persist()
	rf.tracef("Sending initial heartbeat")
	rf.broadcastAppendEntriesAsync()
	heartbeatProvider := makeHeartbeat(len(rf.peers), 110*time.Millisecond)
	defer heartbeatProvider.kill()
	responseReceivedMarker := make([]bool, len(rf.peers)) // any new response obtained since last send.
	dirtyMarker := make([]bool, len(rf.peers))            // any new requests have arrived since last send.
	for i := 0; i < len(responseReceivedMarker); i++ {
		responseReceivedMarker[i] = false
		dirtyMarker[i] = false
	}
	//heartBeat := heartbeatTimeout()
	for {
		select {
		case ev := <-rf.eventQueue:
			if rf.killed() {
				rf.tryAbortEvent(&ev)
				return
			}
			switch ev := ev.(type) {
			case EventTakeSnapshot:
				rf.compactLogs(ev.realID, ev.snapshot)
				ev.done()
			case EventCondSnapshotInstall:
				*ev.success = rf.condSnapshotInstall(ev.lastIncludedTerm, ev.lastIncludedIndex, ev.snapshot)
				ev.done()
			case EventAppendEvent:
				realID := rf.getLastRealID() + 1
				rf.persistent.Log = append(rf.persistent.Log, Log{RealID: realID, Term: rf.persistent.getCurrentTerm(), Data: ev.command})

				rf.persist()
				*ev.isLeader = true
				*ev.term = rf.persistent.getCurrentTerm()
				*ev.index = realID
				lastLogID, _, _ := rf.getLastLogInfo()

				if TraceVerbose {
					rf.tracef("Inserting log %d(id=%d, %v).", lastLogID, realID, ev.command)
				}
				if lastLogID <= rf.volatile.commitIndex {
					panic("Trying to override committed index!")
				}
				ev.done()
				for i := 0; i < len(rf.peers); i++ {
					dirtyMarker[i] = true
					if responseReceivedMarker[i] {
						rf.sendAppendEntriesAsync(i)
						responseReceivedMarker[i] = false
						dirtyMarker[i] = false
					}
				}
			case EventAppendEntriesReq:
				req := ev.req
				if req.Term == rf.persistent.getCurrentTerm() {
					panic("Split brain detected.")
				}
				if req.Term < rf.persistent.getCurrentTerm() {
					*ev.res = AppendEntriesReply{
						Term:    rf.persistent.getCurrentTerm(),
						Success: false,
					}
					ev.done()
					continue
				}
				if req.Term > rf.persistent.getCurrentTerm() {
					rf.becomeFollower(req.Term, -1)
					res := rf.tryAppendLogs(req)
					*ev.res = *res
					ev.done()
					return
				}
			case EventAppendEntriesRes:
				res := ev.res
				// Regular response.
				if res.Term == rf.persistent.getCurrentTerm() {
					immediateRetry := dirtyMarker[ev.id]
					if ev.source.IsInstallSnapshot { // Install snapshot success
						// set matchindex to at least success value.
						newMatch := ev.source.LastIncludedIndex
						if newMatch > rf.volatileLeader.matchIndex[ev.id] {
							rf.volatileLeader.matchIndex[ev.id] = newMatch
							rf.leaderUpdateMatchIndex()
						}
						rf.volatileLeader.nextIndex[ev.id] = rf.volatileLeader.matchIndex[ev.id] + 1
						immediateRetry = true
					} else {
						if res.Success {
							if ev.source.PrevLogIndex+len(ev.source.Entries) > rf.volatileLeader.matchIndex[ev.id] {
								rf.volatileLeader.matchIndex[ev.id] = ev.source.PrevLogIndex + len(ev.source.Entries)
								rf.leaderUpdateMatchIndex()
							}
							rf.volatileLeader.nextIndex[ev.id] = rf.volatileLeader.matchIndex[ev.id] + 1
						} else {
							rf.tracef("AppendEntries to %d failed.", ev.id)
							// Be careful: the log may has already been snapshotted.
							if res.IsConflict {
								i := -1
								for i = ev.source.PrevLogIndex; i >= res.ConflictIndex && i >= rf.persistent.SnapshotLastIncludedIndex; i-- {
									inLogI := rf.getInLogIndex(i)
									inLogTerm := rf.persistent.SnapshotLastIncludedTerm
									if inLogI != -1 {
										inLogTerm = rf.persistent.Log[inLogI].Term
									}
									if inLogTerm == res.ConflictTerm {
										break // A matching index found.
									}
								}
								// i is the log entry that probably equals in term.
								rf.volatileLeader.nextIndex[ev.id] = i + 1
							} else {
								rf.tracef("Because the peer only has log up to %d", res.ConflictIndex)
								rf.volatileLeader.nextIndex[ev.id] = res.ConflictIndex + 1
							}
							//rf.volatileLeader.nextIndex[ev.id]--
							if rf.volatileLeader.nextIndex[ev.id] < 0 {
								panic("Bad nextIndex")
							}
							immediateRetry = true
						}
					}
					responseReceivedMarker[ev.id] = true
					if immediateRetry {
						rf.tracef("Immediately retry with a newer nextIndex[%d] = %d", ev.id, rf.volatileLeader.nextIndex[ev.id])
						rf.sendAppendEntriesAsync(ev.id)
						responseReceivedMarker[ev.id] = false
						dirtyMarker[ev.id] = false
					}

				}
				// Otherwise, this comes from a delayed reply.
				// Need update.
				if res.Term > rf.persistent.getCurrentTerm() {
					// Immediately falls to follower.
					rf.becomeFollower(res.Term, -1)
					return
				}
			case EventRequestVoteReq:
				req := ev.req
				// Check if there is a new election.
				if req.Term > rf.persistent.getCurrentTerm() {
					rf.tracef("Larger term %d spotted. I'm stepping down leader.", req.Term)
					// Immediately falls to follower.
					if rf.electionProperty(req) {
						rf.tracef("I voted #%d.", req.CandidateID)
						// Vote for the little cute candidate.
						rf.becomeFollower(req.Term, req.CandidateID)
						*ev.res = RequestVoteReply{
							Term:        req.Term,
							VoteGranted: true,
						}
					} else {
						rf.tracef("But #%d violates election property.", req.CandidateID)
						// Nope. Your log is not new enough.
						rf.becomeFollower(req.Term, -1)
						*ev.res = RequestVoteReply{
							Term:        req.Term,
							VoteGranted: false,
						}
					}
					ev.done()
					return
				} else {
					// Nope. I'm your leader.
					ev.res.VoteGranted = false
					ev.res.Term = rf.persistent.getCurrentTerm()
					ev.done()
				}
			case EventRequestVoteRes:
				res := ev.res
				if res.Term > rf.persistent.getCurrentTerm() {
					// New term spotted. Become follower.
					rf.becomeFollower(res.Term, -1)
					return
				} else {
					// Thank you. But I don't need the ticket any more.
				}

			default:
				panic("Bad RPC message type.")
			}
		case peerID := <-heartbeatProvider.C:
			if rf.killed() {
				return
			}
			rf.sendAppendEntriesAsync(peerID)
			responseReceivedMarker[peerID] = false
			dirtyMarker[peerID] = false
			// the corresponding timer is automatically reset by recv.
		case <-rf.killer:
			return
		}
	}
}

func (rf *Raft) termFollower() {
	timeout, dur := electionTimeout()
	rf.tracef("Election timeout set to %d", dur)
	for {

		select {
		case ev := <-rf.eventQueue:
			if rf.killed() {
				rf.tryAbortEvent(&ev)
				return
			}
			switch ev := ev.(type) {
			case EventTakeSnapshot:
				rf.compactLogs(ev.realID, ev.snapshot)
				ev.done()
			case EventCondSnapshotInstall:
				*ev.success = rf.condSnapshotInstall(ev.lastIncludedTerm, ev.lastIncludedIndex, ev.snapshot)
				ev.done()
			case EventAppendEvent:
				ev.abort()
			case EventAppendEntriesReq:
				req := ev.req
				if req.Term < rf.persistent.getCurrentTerm() {
					// Nope.
					*ev.res = AppendEntriesReply{
						Term:    rf.persistent.getCurrentTerm(),
						Success: false,
					}
					ev.done()
					continue
				}
				if req.Term >= rf.persistent.getCurrentTerm() {
					needReset := false
					if req.Term > rf.persistent.getCurrentTerm() {
						// First update term.
						rf.becomeFollower(req.Term, -1)
						needReset = true
					}
					// Try to append the logs.
					res := rf.tryAppendLogs(req)
					*ev.res = *res
					ev.done()
					if needReset {
						return
					} else {
						timeout, dur = electionTimeout()
						rf.tracef("Election timeout set to %d", dur)
					}
				}

			case EventAppendEntriesRes:
				res := ev.res
				if res.Term > rf.persistent.getCurrentTerm() {
					rf.becomeFollower(res.Term, -1)
					return
				}
			case EventRequestVoteReq:
				req := ev.req
				if req.Term < rf.persistent.getCurrentTerm() {
					// Get outta here.
					*ev.res = RequestVoteReply{
						Term:        rf.persistent.getCurrentTerm(),
						VoteGranted: false,
					}
					ev.done()
					continue
				}
				// Check if there is a new election.
				if req.Term > rf.persistent.getCurrentTerm() {
					rf.tracef("Larger term %d spotted. Renewing term.", req.Term)
					rf.becomeFollower(req.Term, -1)
				}
				if rf.persistent.VotedFor == -1 {
					// Immediately falls to follower.
					if rf.electionProperty(req) {
						rf.tracef("Voting for %d.", req.CandidateID)
						// Vote for the little cute candidate.
						rf.becomeFollower(req.Term, req.CandidateID)
						*ev.res = RequestVoteReply{
							Term:        req.Term,
							VoteGranted: true,
						}
						timeout, dur = electionTimeout()
						rf.tracef("Election timeout set to %d", dur)
					} else {
						rf.tracef("Not voting, since %d violates election property.", req.CandidateID)
						// Nope. Your log is not new enough.
						rf.becomeFollower(req.Term, -1)
						*ev.res = RequestVoteReply{
							Term:        req.Term,
							VoteGranted: false,
						}
					}
					ev.done()
				} else {
					rf.tracef("Not voting, since we have voted for %d.", rf.persistent.VotedFor)
					// Nope. I have voted for someone else.
					*ev.res = RequestVoteReply{
						Term:        rf.persistent.getCurrentTerm(),
						VoteGranted: false,
					}
					ev.done()
				}

			case EventRequestVoteRes:
				res := ev.res
				if res.Term > rf.persistent.getCurrentTerm() {
					rf.tracef("New term follower.")
					rf.becomeFollower(res.Term, -1)
					return
				}
			default:
				panic("Bad RPC message type.")
			}
		// Election for every event.
		case <-timeout:
			if rf.killed() {
				return
			}
			rf.setStatus(Candidate)
			return
		case <-rf.killer:
			return
		}
	}
}

func (rf *Raft) termCandidate() {
	rf.persistent.setCurrentTerm(rf.persistent.getCurrentTerm() + 1)
	rf.persistent.VotedFor = rf.me
	rf.persist()
	lli, llt, _ := rf.getLastLogInfo()

	// Request vote for myself.
	rf.broadcastRequestVoteAsync(&RequestVoteArgs{
		Term:         rf.persistent.getCurrentTerm(),
		CandidateID:  rf.me,
		LastLogIndex: lli,
		LastLogTerm:  llt,
	})
	timeout, dur := electionTimeout()
	rf.tracef("Election timeout set to %d", dur)
	supporterCounter := 1
	supporterThreshold := (len(rf.peers) + 1) / 2
	for {
		select {
		case ev := <-rf.eventQueue:
			if rf.killed() {
				rf.tryAbortEvent(&ev)
				return
			}
			switch ev := ev.(type) {
			case EventTakeSnapshot:
				rf.compactLogs(ev.realID, ev.snapshot)
				ev.done()
			case EventCondSnapshotInstall:
				*ev.success = rf.condSnapshotInstall(ev.lastIncludedTerm, ev.lastIncludedIndex, ev.snapshot)
				ev.done()
			case EventAppendEvent:
				ev.abort()
			case EventAppendEntriesReq:
				req := ev.req
				if req.Term < rf.persistent.getCurrentTerm() {
					// Nope.
					*ev.res = AppendEntriesReply{
						Term:    rf.persistent.getCurrentTerm(),
						Success: false,
					}
					ev.done()
					continue
				}
				if req.Term >= rf.persistent.getCurrentTerm() {
					if req.Term > rf.persistent.getCurrentTerm() {
						rf.becomeFollower(req.Term, -1)
					} else {
						rf.becomeFollower(req.Term, rf.persistent.VotedFor)
					}
					res := rf.tryAppendLogs(req)
					*ev.res = *res
					ev.done()
					return
				}
			case EventAppendEntriesRes:
				res := ev.res
				/*if res.Term == rf.persistent.getCurrentTerm() {
					panic("Don't look at me. I'm not a split brain.")
				}*/
				if res.Term > rf.persistent.getCurrentTerm() {
					rf.becomeFollower(res.Term, -1)
					return
				}

			case EventRequestVoteReq:
				req := ev.req
				if req.Term < rf.persistent.getCurrentTerm() {
					// Get outta here.
					*ev.res = RequestVoteReply{
						Term:        rf.persistent.getCurrentTerm(),
						VoteGranted: false,
					}
					ev.done()
					continue
				}
				// Check if there is a new election.
				if req.Term > rf.persistent.getCurrentTerm() {
					rf.becomeFollower(req.Term, -1)
					// Immediately falls to follower.
					if rf.electionProperty(req) {
						// Vote for the little cute candidate.
						rf.tracef("Voting for %d.", req.CandidateID)
						rf.becomeFollower(req.Term, req.CandidateID)
						*ev.res = RequestVoteReply{
							Term:        req.Term,
							VoteGranted: true,
						}
					} else {
						rf.tracef("Not voting, since %d log not enough.", req.CandidateID)
						// Nope. Your log is not new enough.
						rf.becomeFollower(req.Term, -1)
						*ev.res = RequestVoteReply{
							Term:        req.Term,
							VoteGranted: false,
						}
					}
					ev.done()
					return
				} else {
					// Nope. I have voted for myself.
					rf.tracef("Not voting for %d, since I've voted for myself.", req.CandidateID)
					*ev.res = RequestVoteReply{
						Term:        rf.persistent.getCurrentTerm(),
						VoteGranted: false,
					}
					ev.done()
				}

			case EventRequestVoteRes:
				if ev.res.Term > rf.persistent.getCurrentTerm() {
					rf.becomeFollower(ev.res.Term, -1)
					return
				}
				if ev.res.Term == rf.persistent.getCurrentTerm() && ev.res.VoteGranted {
					// Thank you for your vote.
					supporterCounter++
					rf.tracef("Vode %d/%d", supporterCounter, supporterThreshold)
					if supporterCounter >= supporterThreshold {
						// Long live our new king!
						rf.tracef("I become the leader.")
						rf.setStatus(Leader)
						return
					}
				}
			}
		case <-timeout:
			if rf.killed() {
				return
			}
			// Next candidate term.
			return
		case <-rf.killer:
			// Killed.
			return
		}
	}
}

func (rf *Raft) getRoleString() string {
	switch rf.getStatus() {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	}
	return "<unknown>"
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.becomeFollower(rf.persistent.getCurrentTerm(), -1)
	for rf.killed() == false {
		// We place literally everything in one goroutine.
		// In this case we don't need to care about locking anymore.

		switch rf.getStatus() {
		case Leader:
			rf.tracef("Today is term #%d, a %s term.", rf.persistent.getCurrentTerm(), rf.getRoleString())
			rf.termLeader()
		case Follower:
			rf.tracef("Today is term #%d, a %s term.", rf.persistent.getCurrentTerm(), rf.getRoleString())
			rf.termFollower()
		case Candidate:
			rf.tracef("Today is term #%d, a %s term.", rf.persistent.getCurrentTerm()+1, rf.getRoleString())
			rf.termCandidate()
		}
	}
	rf.tracef("Graceful shutdown.")
	// Now that channel shut down, we scan through all events.
CLEAN_EV:
	for {
		select {
		case ev := <-rf.eventQueue:
			rf.tryAbortEvent(&ev)
		default:
			close(rf.eventQueue)
			close(rf.asyncApplyCh)
			break CLEAN_EV
		}
	}
	rf.tracef("All coroutines killed.")
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	labgob.Register(CommandNop{})
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.volatile = RaftVolatileInfoAll{
		commitIndex: -1,
		lastApplied: -1,
	}
	rf.killer = make(chan int, 1)
	rf.eventQueue = make(chan interface{}, 1)
	rf.applyCh = applyCh
	rf.rwmuKilled = &sync.RWMutex{}
	rf.asyncApplyCh = make(chan *ApplyMsg)
	rf.snapshotRealIDMap = make(map[CSIKey]CSIValue)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.currentSnapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.applier()
	go rf.ticker()

	return rf
}

const (
	ApplierPending int = 10
)

// The magic applier that applies tasks.
func (rf *Raft) applier() {
	slowreq := make(chan *ApplyMsg, ApplierPending)
	slowres := make(chan int, ApplierPending)
	worker := func() {
		for req := range slowreq {
			if req == nil {
				panic("Bad request")
			}
			rf.applyCh <- *req
			slowres <- 0
		}
	}
	workerFree := ApplierPending
	taskQueue := make([]*ApplyMsg, 0)
	go worker()
	for {
		select {
		case req, ok := <-rf.asyncApplyCh:
			if !ok {
				close(slowreq)
				return
			}
			if workerFree > 0 {
				slowreq <- req
				workerFree--
			} else {
				taskQueue = append(taskQueue, req)
			}
		case <-slowres:
			workerFree++
			if workerFree > ApplierPending {
				panic("Bad worker state")
			}
			for len(taskQueue) > 0 && workerFree > 0 {
				x := taskQueue[0]
				taskQueue = taskQueue[1:]
				slowreq <- x
				workerFree--
			}

		}
	}

}
