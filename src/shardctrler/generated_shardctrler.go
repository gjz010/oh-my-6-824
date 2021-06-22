package shardctrler

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

func (state *ShardController_State) TransferAction_Join(args ShardController_Action_Args_Join) ShardController_Action_Reply_Join {
	return state.TransferAction_Join_Impl(args)
}
func (state *ShardController_State) TransferAction_Leave(args ShardController_Action_Args_Leave) ShardController_Action_Reply_Leave {
	return state.TransferAction_Leave_Impl(args)
}
func (state *ShardController_State) TransferAction_Move(args ShardController_Action_Args_Move) ShardController_Action_Reply_Move {
	return state.TransferAction_Move_Impl(args)
}
func (state *ShardController_State) TransferAction_Query(args ShardController_Action_Args_Query) ShardController_Action_Reply_Query {
	return state.TransferAction_Query_Impl(args)
}

type ShardController_Action_Args_Join struct {
	Servers map[int][]string
}
type ShardController_Action_Reply_Join struct {
	Magic int
}
type ShardController_Action_Args_Leave struct {
	GIDs []int
}
type ShardController_Action_Reply_Leave struct {
	Magic int
}
type ShardController_Action_Args_Move struct {
	Shard int
	GID   int
}
type ShardController_Action_Reply_Move struct {
	Magic int
}
type ShardController_Action_Args_Query struct {
	Num int
}
type ShardController_Action_Reply_Query struct {
	Config Config
}

func (ck *ShardController_Clerk) Action_Join(op ShardController_Action_Args_Join) (*ShardController_Action_Reply_Join, bool) {
	descriptor := ShardController_OperationDescriptor{
		ValidID:  1,
		Val_Join: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor)
	if err {
		return nil, true
	}
	if ret.ValidID != 1 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_Join, false
}
func (ck *ShardController_Clerk) Action_Leave(op ShardController_Action_Args_Leave) (*ShardController_Action_Reply_Leave, bool) {
	descriptor := ShardController_OperationDescriptor{
		ValidID:   2,
		Val_Leave: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor)
	if err {
		return nil, true
	}
	if ret.ValidID != 2 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_Leave, false
}
func (ck *ShardController_Clerk) Action_Move(op ShardController_Action_Args_Move) (*ShardController_Action_Reply_Move, bool) {
	descriptor := ShardController_OperationDescriptor{
		ValidID:  3,
		Val_Move: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor)
	if err {
		return nil, true
	}
	if ret.ValidID != 3 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_Move, false
}
func (ck *ShardController_Clerk) Action_Query(op ShardController_Action_Args_Query) (*ShardController_Action_Reply_Query, bool) {
	descriptor := ShardController_OperationDescriptor{
		ValidID:   4,
		Val_Query: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor)
	if err {
		return nil, true
	}
	if ret.ValidID != 4 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_Query, false
}
func (kv *ShardController_RSMState) PerformActions(op *ShardController_OperationDescriptor, ret *ShardController_ReturnValueDescriptor) {
	ret.ValidID = op.ValidID
	switch op.ValidID {
	case 1:
		reply := kv.State.TransferAction_Join(*op.Val_Join)
		ret.Ret_Join = &reply
		break
	case 2:
		reply := kv.State.TransferAction_Leave(*op.Val_Leave)
		ret.Ret_Leave = &reply
		break
	case 3:
		reply := kv.State.TransferAction_Move(*op.Val_Move)
		ret.Ret_Move = &reply
		break
	case 4:
		reply := kv.State.TransferAction_Query(*op.Val_Query)
		ret.Ret_Query = &reply
		break
	}
}

type ShardController_OperationDescriptor struct {
	ValidID   int
	Val_Join  *ShardController_Action_Args_Join
	Val_Leave *ShardController_Action_Args_Leave
	Val_Move  *ShardController_Action_Args_Move
	Val_Query *ShardController_Action_Args_Query
}
type ShardController_ReturnValueDescriptor struct {
	ValidID   int
	Ret_Join  *ShardController_Action_Reply_Join
	Ret_Leave *ShardController_Action_Reply_Leave
	Ret_Move  *ShardController_Action_Reply_Move
	Ret_Query *ShardController_Action_Reply_Query
}
type ShardController_State struct {
	ConfigVersions []Config
}

func New_ShardController_State() ShardController_State {
	return ShardController_State{
		ConfigVersions: InitialConfig(),
	}
}

// Template below. No modification needed.

type ShardController_Op struct {
	RealOp       ShardController_OperationDescriptor
	ClientId     int64
	ClientSerial int64
}

type ShardController_EventResponse struct {
	IsOk               bool
	IsAlreadyCommitted bool
	IsClearedRequest   bool
	IsKilled           bool
	Value              ShardController_ReturnValueDescriptor
}

type ShardController_EventOp struct {
	req *ShardController_Op
	res chan ShardController_EventResponse
}

type ShardController_RSMState struct {
	State                      ShardController_State
	CommittedIndicesDontAccess map[int64]int64
}

func (kv *ShardController_RSMState) getCommittedIndex(clientId int64) int64 {
	val, ok := kv.CommittedIndicesDontAccess[clientId]
	if ok {
		return val
	} else {
		return -1
	}
}

func (kv *ShardController_RSMState) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*kv)
	data := w.Bytes()
	return data
}

func ShardController_deserializeKVStore(data []byte) *ShardController_RSMState {
	newStore := ShardController_RSMState{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&newStore) != nil {
		panic("Bad state")
	}
	return &newStore
}

type ShardController_ApplyLogResult struct {
	status string
	value  ShardController_ReturnValueDescriptor
}

// this is a pure function.
// deterministic state transfer here.
func (kv *ShardController_RSMState) applyLog(log *ShardController_Op) ShardController_ApplyLogResult {
	result := ShardController_ApplyLogResult{
		status: "Ok",
		value:  ShardController_ReturnValueDescriptor{},
	}
	if log.ClientSerial <= kv.getCommittedIndex(log.ClientId) {
		// already committed
		result.status = "Outdated"
		return result
	}
	kv.PerformActions(&log.RealOp, &result.value)
	/* ShardController_ApplyLogOperation_Begin */
	/* ShardController_ApplyLogOperation_End */
	kv.CommittedIndicesDontAccess[log.ClientId] = log.ClientSerial
	return result
}

type ShardController_ClientReq struct {
	ClientId     int64
	ClientSerial int64
}
type ShardController_ReplicatedStateMachine struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	rwdead  sync.RWMutex
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore         *ShardController_RSMState
	eventQueue      chan ShardController_EventOp
	killChannel     chan int
	lastSeenTerm    int
	pendingRequests map[ShardController_ClientReq]chan ShardController_EventResponse
	persister       *raft.Persister
}

const (
	ShardController_TraceEnabled       = false
	ShardController_ClientTraceEnabled = false
)

func (kv *ShardController_ReplicatedStateMachine) tracef(msg string, args ...interface{}) {
	if ShardController_TraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[Shrd][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), kv.me, m)
	}
}

func (kv *ShardController_ReplicatedStateMachine) pushEvent(ev ShardController_EventOp) bool {
	kv.rwdead.RLock()
	flag := false
	if !kv.killed() {
		kv.eventQueue <- ev
		flag = true
	}
	kv.rwdead.RUnlock()
	return flag
}

type ShardController_ActionArgs struct {
	RealOp       ShardController_OperationDescriptor
	ClientId     int64
	ClientSerial int64
}
type ShardController_ActionReply struct {
	Value ShardController_ReturnValueDescriptor
	Err   Err
}

func (kv *ShardController_ReplicatedStateMachine) ShardController_Action(args *ShardController_ActionArgs, reply *ShardController_ActionReply) {
	//kv.tracef("Get Req %+v", *args)
	req := ShardController_Op{
		RealOp:       args.RealOp,
		ClientId:     args.ClientId,
		ClientSerial: args.ClientSerial,
	}
	responseChannel := make(chan ShardController_EventResponse, 1)
	event := ShardController_EventOp{
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

func (kv *ShardController_ReplicatedStateMachine) Kill() {
	kv.rwdead.Lock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killChannel <- 114514
	kv.rwdead.Unlock()
	// Your code here, if desired.
}

func (kv *ShardController_ReplicatedStateMachine) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardController_ReplicatedStateMachine) performRaft(log *ShardController_Op) (bool, bool, int) {
	index, observedTerm, isLeader := kv.rf.Start(*log)
	termUpdated := observedTerm > kv.lastSeenTerm
	//kv.tracef("Term check (raft) %d->%d", kv.lastSeenTerm, observedTerm)
	if observedTerm > kv.lastSeenTerm {
		kv.lastSeenTerm = observedTerm
	}
	return isLeader, termUpdated, index
}

// Returns OK, ErrWrongLeader or ErrOutdatedGet.
func (kv *ShardController_ReplicatedStateMachine) tryPerformRaftAndClearup(log *ShardController_Op, res chan ShardController_EventResponse) (bool, int) {
	if kv.kvstore.getCommittedIndex(log.ClientId) >= log.ClientSerial {
		res <- ShardController_EventResponse{IsAlreadyCommitted: true}
	}
	reqID := ShardController_ClientReq{ClientId: log.ClientId, ClientSerial: log.ClientSerial}
	val, ok := kv.pendingRequests[reqID]
	if ok {
		val <- ShardController_EventResponse{IsClearedRequest: true}
		delete(kv.pendingRequests, reqID)
	}
	isLeader, termUpdated, index := kv.performRaft(log)
	if termUpdated {
		kv.clearPendingRequestsByTermChange()
	}
	if !isLeader {
		//kv.tracef("Not leader")
		res <- ShardController_EventResponse{}
		return false, 0
	}
	kv.tracef("A new request %+v", *log)

	kv.pendingRequests[reqID] = res
	return true, index
}

func (kv *ShardController_ReplicatedStateMachine) clearPendingRequestsByTermChange() {
	m := kv.pendingRequests
	for _, ch := range m {
		ch <- ShardController_EventResponse{}
	}
	kv.pendingRequests = make(map[ShardController_ClientReq]chan ShardController_EventResponse)
}
func (kv *ShardController_ReplicatedStateMachine) printAllPendingRequests(term int) {
	keys := make([]ShardController_ClientReq, 0, len(kv.pendingRequests))
	for k := range kv.pendingRequests {
		keys = append(keys, k)
	}
	kv.tracef("Pending requests for term %d: %+v", term, keys)
}
func (kv *ShardController_ReplicatedStateMachine) eventLoop() {
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
				case ShardController_Op:
					if !(msg.CommandValid && (!msg.SnapshotValid)) {
						log.Panic("Bad state")
					}
					kv.tracef("Applying op %+v", op)
					ret := kv.kvstore.applyLog(&op)
					kv.tracef("Applied op %+v = %+v", op, ret)
					reqID := ShardController_ClientReq{ClientId: op.ClientId, ClientSerial: op.ClientSerial}

					val, ok := kv.pendingRequests[reqID]
					if ok {
						if ret.status == "Outdated" {
							val <- ShardController_EventResponse{IsAlreadyCommitted: true}
						} else {
							val <- ShardController_EventResponse{IsOk: true, Value: ret.value}
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
					kv.kvstore = ShardController_deserializeKVStore(msg.Snapshot)
				}
			}
			newTerm, _ := kv.rf.GetState()
			//kv.tracef("Term check (apply) %d->%d", kv.lastSeenTerm, newTerm)
			if newTerm > kv.lastSeenTerm {
				kv.lastSeenTerm = newTerm
				kv.clearPendingRequestsByTermChange()
			} else {
				// in either case, remove overcommitted request.
				newPendingRequests := make(map[ShardController_ClientReq]chan ShardController_EventResponse)
				// remove overcommitted request
				for reqID, ch := range kv.pendingRequests {
					if kv.kvstore.getCommittedIndex(reqID.ClientId) >= reqID.ClientSerial {
						ch <- ShardController_EventResponse{IsAlreadyCommitted: true}
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
			remainEv.res <- ShardController_EventResponse{IsKilled: true}
		default:
			break GRACEFUL_SHUTDOWN
		}
	}
	for _, pending := range kv.pendingRequests {
		pending <- ShardController_EventResponse{IsKilled: true}
	}
}

func ShardController_StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *ShardController_ReplicatedStateMachine {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(ShardController_Op{})

	kv := new(ShardController_ReplicatedStateMachine)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	snapshot := kv.persister.ReadSnapshot()

	if snapshot != nil && len(snapshot) != 0 {
		kv.kvstore = ShardController_deserializeKVStore(snapshot)
	} else {
		kv.kvstore = &ShardController_RSMState{State: New_ShardController_State(),
			CommittedIndicesDontAccess: make(map[int64]int64)}
	}

	kv.eventQueue = make(chan ShardController_EventOp)
	kv.killChannel = make(chan int, 1)
	kv.lastSeenTerm = -1
	kv.pendingRequests = make(map[ShardController_ClientReq]chan ShardController_EventResponse)
	kv.dead = 0

	// You may need initialization code here.
	go kv.eventLoop()
	return kv
}

type ShardController_Clerk struct {
	servers                                     []*labrpc.ClientEnd
	lastLeader                                  int
	clientId                                    int64
	clientSerial                                int64
	lockSoThatClientWillNotSendMultipleRequests sync.Mutex
	// You will have to modify this struct.
}

func (ck *ShardController_Clerk) getNextSerial() int64 {
	serial := ck.clientSerial
	ck.clientSerial++
	return serial
}

func ShardController_MakeClerk(servers []*labrpc.ClientEnd) *ShardController_Clerk {
	ck := new(ShardController_Clerk)
	ck.servers = servers
	ck.clientSerial = 0
	ck.clientId = nrand()
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

func (ck *ShardController_Clerk) tracef(msg string, args ...interface{}) {
	if ShardController_ClientTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[SCCl][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), ck.clientId, m)
	}
}

func (ck *ShardController_Clerk) PerformGeneralAction(op ShardController_OperationDescriptor) (*ShardController_ReturnValueDescriptor, bool) {
	ck.lockSoThatClientWillNotSendMultipleRequests.Lock()
	defer ck.lockSoThatClientWillNotSendMultipleRequests.Unlock()
	leader := ck.lastLeader
	args := ShardController_ActionArgs{RealOp: op, ClientId: ck.clientId, ClientSerial: ck.getNextSerial()}
	reply := ShardController_ActionReply{}
	ck.tracef("Sending request %+v", args)
OP_LOOP:
	for {
		ck.tracef("Trying server %d", leader)
		ok := ck.servers[leader].Call("ShardCtrler.ShardController_Action", &args, &reply)
		if !ok {
			ck.tracef("Sending request %+v failed.", args)
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
			continue // try again
		}
		switch reply.Err {
		case OK:
			break OP_LOOP
		case ErrKilled:
			ck.tracef("Killed")
			reply = ShardController_ActionReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrWrongLeader:
			ck.tracef("Wrong leader")
			reply = ShardController_ActionReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrOutdatedOp:
			return nil, true
		case ErrButItIsYouWhoTimedoutFirst:
			log.Panicln("This should not happen.")
		}
	}

	ck.lastLeader = leader
	val := reply.Value
	return &val, false
}
