package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType OpType
	Key           string
	Value         string
	ClientId      int64
	ClientSerial  int64
}
type OpType string

const (
	OpPut    OpType = "OpPut"
	OpAppend        = "OpAppend"
	OpGet           = "OpGet"
)

type EventResponse struct {
	IsOk               bool
	IsAlreadyCommitted bool
	IsClearedRequest   bool
	IsKilled           bool
	Value              string
}

type EventOp struct {
	req *Op
	res chan EventResponse
}

type KVStore struct {
	Store                      map[string]string
	CommittedIndicesDontAccess map[int64]int64
}

func (kv *KVStore) getCommittedIndex(clientId int64) int64 {
	val, ok := kv.CommittedIndicesDontAccess[clientId]
	if ok {
		return val
	} else {
		return -1
	}
}

func (kv *KVStore) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*kv)
	data := w.Bytes()
	return data
}

func deserializeKVStore(data []byte) *KVStore {
	newStore := KVStore{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&newStore) != nil {
		panic("Bad state")
	}
	return &newStore
}

type ApplyLogResult struct {
	status string
	value  string
}

// this is a pure function.
// deterministic state transfer here.
func (kv *KVStore) applyLog(log *Op) ApplyLogResult {
	result := ApplyLogResult{
		status: "Ok",
		value:  "",
	}
	if log.ClientSerial <= kv.getCommittedIndex(log.ClientId) {
		// already committed
		result.status = "Outdated"
		return result
	}
	switch log.OperationType {
	case OpPut:
		kv.Store[log.Key] = log.Value
		result.status = "Put"
		result.value = kv.Store[log.Key]
		break
	case OpAppend:
		kv.Store[log.Key] += log.Value
		result.status = "Append"
		result.value = kv.Store[log.Key]
		break
	case OpGet:
		result.status = "Get"
		result.value = kv.Store[log.Key]
		break
	}
	kv.CommittedIndicesDontAccess[log.ClientId] = log.ClientSerial
	return result
}

type ClientReq struct {
	ClientId     int64
	ClientSerial int64
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	rwdead  sync.RWMutex
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore         *KVStore
	eventQueue      chan EventOp
	killChannel     chan int
	lastSeenTerm    int
	pendingRequests map[ClientReq]chan EventResponse
	persister       *raft.Persister
}

const (
	TraceEnabled       = true
	ClientTraceEnabled = false
)

func (kv *KVServer) tracef(msg string, args ...interface{}) {
	if TraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[KVSt][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), kv.me, m)
	}
}

func (kv *KVServer) pushEvent(ev EventOp) bool {
	kv.rwdead.RLock()
	flag := false
	if !kv.killed() {
		kv.eventQueue <- ev
		flag = true
	}
	kv.rwdead.RUnlock()
	return flag
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//kv.tracef("Get Req %+v", *args)
	req := Op{
		OperationType: OpGet,
		Key:           args.Key,
		Value:         "",
		ClientId:      args.ClientId,
		ClientSerial:  args.ClientSerial,
	}
	responseChannel := make(chan EventResponse, 1)
	event := EventOp{
		req: &req,
		res: responseChannel,
	}
	success := kv.pushEvent(event)
	if !success {
		reply.Err = ErrKilled
		return
	}
	result := <-responseChannel
	if result.IsAlreadyCommitted {
		reply.Err = ErrOutdatedOp
		return
	} else if result.IsOk {
		reply.Value = result.Value
		reply.Err = OK
		return
	} else {
		if result.IsClearedRequest {
			reply.Err = ErrButItIsYouWhoTimedoutFirst
		} else if result.IsKilled {
			reply.Err = ErrKilled
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//kv.tracef("PutAppend Req %+v", *args)
	LogOp := OpPut
	if args.Op == "Put" {
		LogOp = OpPut
	} else {
		LogOp = OpAppend
	}
	responseChannel := make(chan EventResponse, 1)
	req := Op{
		OperationType: LogOp,
		Key:           args.Key,
		Value:         args.Value,
		ClientId:      args.ClientId,
		ClientSerial:  args.ClientSerial,
	}
	event := EventOp{
		req: &req,
		res: responseChannel,
	}
	success := kv.pushEvent(event)
	if !success {
		reply.Err = ErrKilled
		return
	}
	result := <-responseChannel
	if result.IsAlreadyCommitted || result.IsOk {
		reply.Err = OK
		return
	}
	if result.IsClearedRequest {
		reply.Err = ErrButItIsYouWhoTimedoutFirst
	} else if result.IsKilled {
		reply.Err = ErrKilled
	} else {
		reply.Err = ErrWrongLeader
	}

	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	kv.rwdead.Lock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killChannel <- 114514
	kv.rwdead.Unlock()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Perform raft operation check term-change.
// First return value means if the node is leader (i.e. is the submission successful). Second return value means if the term is updated.
func (kv *KVServer) performRaft(log *Op) (bool, bool, int) {
	index, observedTerm, isLeader := kv.rf.Start(*log)
	termUpdated := observedTerm > kv.lastSeenTerm
	//kv.tracef("Term check (raft) %d->%d", kv.lastSeenTerm, observedTerm)
	if observedTerm > kv.lastSeenTerm {
		kv.lastSeenTerm = observedTerm
	}
	return isLeader, termUpdated, index
}

// Returns OK, ErrWrongLeader or ErrOutdatedGet.
func (kv *KVServer) tryPerformRaftAndClearup(log *Op, res chan EventResponse) (bool, int) {
	if kv.kvstore.getCommittedIndex(log.ClientId) >= log.ClientSerial {
		res <- EventResponse{IsAlreadyCommitted: true}
	}
	reqID := ClientReq{ClientId: log.ClientId, ClientSerial: log.ClientSerial}
	val, ok := kv.pendingRequests[reqID]
	if ok {
		val <- EventResponse{IsClearedRequest: true}
		delete(kv.pendingRequests, reqID)
	}
	isLeader, termUpdated, index := kv.performRaft(log)
	if termUpdated {
		kv.clearPendingRequestsByTermChange()
	}
	if !isLeader {
		//kv.tracef("Not leader")
		res <- EventResponse{}
		return false, 0
	}
	kv.tracef("A new request %+v", *log)

	kv.pendingRequests[reqID] = res
	return true, index
}

func (kv *KVServer) clearPendingRequestsByTermChange() {
	m := kv.pendingRequests
	for _, ch := range m {
		ch <- EventResponse{}
	}
	kv.pendingRequests = make(map[ClientReq]chan EventResponse)
}
func (kv *KVServer) printAllPendingRequests(term int) {
	keys := make([]ClientReq, 0, len(kv.pendingRequests))
	for k := range kv.pendingRequests {
		keys = append(keys, k)
	}
	kv.tracef("Pending requests for term %d: %+v", term, keys)
}
func (kv *KVServer) eventLoop() {
	duration := 100 * time.Millisecond
	checkTerm := time.After(duration)
MAIN_LOOP:
	for !kv.killed() {
		select {
		case <-checkTerm:
			if kv.killed() {
				break MAIN_LOOP
			}
			newTerm, _ := kv.rf.GetState()
			//kv.tracef("Term check (100ms) %d->%d", kv.lastSeenTerm, newTerm)
			if newTerm > kv.lastSeenTerm {
				kv.lastSeenTerm = newTerm
				kv.clearPendingRequestsByTermChange()
			}
			kv.printAllPendingRequests(newTerm)
			checkTerm = time.After(duration)
		case ev := <-kv.eventQueue:
			if kv.killed() {
				break MAIN_LOOP
			}
			kv.tryPerformRaftAndClearup(ev.req, ev.res)
		case msg := <-kv.applyCh:
			if kv.killed() {
				break MAIN_LOOP
			}
			if msg.CommandValid == msg.SnapshotValid {
				log.Panicln("bad apply packet")
			}
			if msg.CommandValid {
				switch op := msg.Command.(type) {
				case Op:
					if !(msg.CommandValid && (!msg.SnapshotValid)) {
						log.Panic("Bad state")
					}

					ret := kv.kvstore.applyLog(&op)
					kv.tracef("Applying op %+v = %+v", op, ret)
					reqID := ClientReq{ClientId: op.ClientId, ClientSerial: op.ClientSerial}
					val, ok := kv.pendingRequests[reqID]
					if ok {
						if ret.status == "Outdated" {
							val <- EventResponse{IsAlreadyCommitted: true}
						} else {
							val <- EventResponse{IsOk: true, Value: ret.value}
						}

						delete(kv.pendingRequests, reqID)
					}
					// try to take snapshot for every applied item
					if kv.maxraftstate > 0 {
						raftSize := kv.persister.RaftStateSize()
						if raftSize >= kv.maxraftstate {
							kv.tracef("Taking snapshot since raftSize = %d, greater than %d", raftSize, kv.maxraftstate)
							kv.rf.Snapshot(msg.CommandIndex, kv.kvstore.serialize())
						}
					}

				default:
					log.Panic("Bad applied ch")
				}
			}
			if msg.SnapshotValid {
				shouldInstall := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
				if shouldInstall {
					// snapshot installation can be seen as installing a bunch of logs.
					kv.kvstore = deserializeKVStore(msg.Snapshot)
				}
			}
			newTerm, _ := kv.rf.GetState()
			//kv.tracef("Term check (apply) %d->%d", kv.lastSeenTerm, newTerm)
			if newTerm > kv.lastSeenTerm {
				kv.lastSeenTerm = newTerm
				kv.clearPendingRequestsByTermChange()
			} else {
				// in either case, remove overcommitted request.
				newPendingRequests := make(map[ClientReq]chan EventResponse)
				// remove overcommitted request
				for reqID, ch := range kv.pendingRequests {
					if kv.kvstore.getCommittedIndex(reqID.ClientId) >= reqID.ClientSerial {
						ch <- EventResponse{IsAlreadyCommitted: true}
					} else {
						newPendingRequests[reqID] = ch
					}
				}
				kv.pendingRequests = newPendingRequests
			}

		case <-kv.killChannel:
			kv.tracef("External killed")
			break MAIN_LOOP
		}
	}
	kv.tracef("Graceful killed")
GRACEFUL_SHUTDOWN:
	for {
		select {
		case remainEv := <-kv.eventQueue:
			remainEv.res <- EventResponse{IsKilled: true}
		default:
			break GRACEFUL_SHUTDOWN
		}
	}
	for _, pending := range kv.pendingRequests {
		pending <- EventResponse{IsKilled: true}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	snapshot := kv.persister.ReadSnapshot()

	if snapshot != nil && len(snapshot) != 0 {
		kv.kvstore = deserializeKVStore(snapshot)
	} else {
		kv.kvstore = &KVStore{Store: make(map[string]string),
			CommittedIndicesDontAccess: make(map[int64]int64)}
	}

	kv.eventQueue = make(chan EventOp)
	kv.killChannel = make(chan int, 1)
	kv.lastSeenTerm = -1
	kv.pendingRequests = make(map[ClientReq]chan EventResponse)
	kv.dead = 0

	// You may need initialization code here.
	go kv.eventLoop()
	return kv
}
