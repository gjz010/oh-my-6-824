package shardkv

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
	"6.824/shardctrler"
)

func (state *ShardKV_State) TransferAction_PutAppend(args ShardKV_Action_Args_PutAppend) ShardKV_Action_Reply_PutAppend {
	return state.TransferAction_PutAppend_Impl(args)
}
func (state *ShardKV_State) TransferAction_Get(args ShardKV_Action_Args_Get) ShardKV_Action_Reply_Get {
	return state.TransferAction_Get_Impl(args)
}
func (state *ShardKV_State) TransferAction_RequestConfigChange(args ShardKV_Action_Args_RequestConfigChange) ShardKV_Action_Reply_RequestConfigChange {
	return state.TransferAction_RequestConfigChange_Impl(args)
}
func (state *ShardKV_State) TransferAction_ReadState(args ShardKV_Action_Args_ReadState) ShardKV_Action_Reply_ReadState {
	return state.TransferAction_ReadState_Impl(args)
}
func (state *ShardKV_State) TransferAction_PullShard(args ShardKV_Action_Args_PullShard) ShardKV_Action_Reply_PullShard {
	return state.TransferAction_PullShard_Impl(args)
}
func (state *ShardKV_State) TransferAction_AckPullShard(args ShardKV_Action_Args_AckPullShard) ShardKV_Action_Reply_AckPullShard {
	return state.TransferAction_AckPullShard_Impl(args)
}
func (state *ShardKV_State) TransferAction_InstallShard(args ShardKV_Action_Args_InstallShard) ShardKV_Action_Reply_InstallShard {
	return state.TransferAction_InstallShard_Impl(args)
}

type ShardKV_Action_Args_PutAppend struct {
	Key          string
	Value        string
	Op           string
	ClientId     int64
	ClientSerial int64
}
type ShardKV_Action_Reply_PutAppend struct {
	WrongGroup    bool
	OutDated      bool
	TryAgainLater bool
	Pong          string
}
type ShardKV_Action_Args_Get struct {
	Key          string
	ClientId     int64
	ClientSerial int64
}
type ShardKV_Action_Reply_Get struct {
	WrongGroup    bool
	OutDated      bool
	TryAgainLater bool
	Value         string
}
type ShardKV_Action_Args_RequestConfigChange struct {
	Config     shardctrler.Config
	NewVersion int
}
type ShardKV_Action_Reply_RequestConfigChange struct {
	CurrentVer int
	Success    bool
}
type ShardKV_Action_Args_ReadState struct {
	Magic int
}
type ShardKV_Action_Reply_ReadState struct {
	MissingShards       []int
	MissingShardSources []int
	CurrentVersion      int
}
type ShardKV_Action_Args_PullShard struct {
	Version int
	Shard   int
}
type ShardKV_Action_Reply_PullShard struct {
	Shard   ShardData
	Success bool
}
type ShardKV_Action_Args_AckPullShard struct {
	Version int
	Shard   int
}
type ShardKV_Action_Reply_AckPullShard struct {
	Magic int
}
type ShardKV_Action_Args_InstallShard struct {
	Data    ShardData
	Version int
	Shard   int
}
type ShardKV_Action_Reply_InstallShard struct {
	Magic int
}

func (ck *ShardKV_Clerk) Action_PutAppend(op ShardKV_Action_Args_PutAppend, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_PutAppend, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID:       1,
		Val_PutAppend: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 1 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_PutAppend, OK
}
func (ck *ShardKV_Clerk) Action_Get(op ShardKV_Action_Args_Get, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_Get, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID: 2,
		Val_Get: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 2 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_Get, OK
}
func (ck *ShardKV_Clerk) Action_RequestConfigChange(op ShardKV_Action_Args_RequestConfigChange, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_RequestConfigChange, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID:                 3,
		Val_RequestConfigChange: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 3 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_RequestConfigChange, OK
}
func (ck *ShardKV_Clerk) Action_ReadState(op ShardKV_Action_Args_ReadState, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_ReadState, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID:       4,
		Val_ReadState: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 4 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_ReadState, OK
}
func (ck *ShardKV_Clerk) Action_PullShard(op ShardKV_Action_Args_PullShard, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_PullShard, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID:       5,
		Val_PullShard: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 5 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_PullShard, OK
}
func (ck *ShardKV_Clerk) Action_AckPullShard(op ShardKV_Action_Args_AckPullShard, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_AckPullShard, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID:          6,
		Val_AckPullShard: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 6 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_AckPullShard, OK
}
func (ck *ShardKV_Clerk) Action_InstallShard(op ShardKV_Action_Args_InstallShard, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_Action_Reply_InstallShard, string) {
	descriptor := ShardKV_OperationDescriptor{
		ValidID:          7,
		Val_InstallShard: &op,
	}
	ret, err := ck.PerformGeneralAction(descriptor, failOnWrongLeader, failOnKilled, failOnBadSend)
	if err != OK {
		return nil, err
	}
	if ret.ValidID != 7 {
		log.Panicln("return type check failed.")
	}
	return ret.Ret_InstallShard, OK
}
func (kv *ShardKV_RSMState) PerformActions(op *ShardKV_OperationDescriptor, ret *ShardKV_ReturnValueDescriptor) {
	ret.ValidID = op.ValidID
	switch op.ValidID {
	case 1:
		reply := kv.State.TransferAction_PutAppend(*op.Val_PutAppend)
		ret.Ret_PutAppend = &reply
		break
	case 2:
		reply := kv.State.TransferAction_Get(*op.Val_Get)
		ret.Ret_Get = &reply
		break
	case 3:
		reply := kv.State.TransferAction_RequestConfigChange(*op.Val_RequestConfigChange)
		ret.Ret_RequestConfigChange = &reply
		break
	case 4:
		reply := kv.State.TransferAction_ReadState(*op.Val_ReadState)
		ret.Ret_ReadState = &reply
		break
	case 5:
		reply := kv.State.TransferAction_PullShard(*op.Val_PullShard)
		ret.Ret_PullShard = &reply
		break
	case 6:
		reply := kv.State.TransferAction_AckPullShard(*op.Val_AckPullShard)
		ret.Ret_AckPullShard = &reply
		break
	case 7:
		reply := kv.State.TransferAction_InstallShard(*op.Val_InstallShard)
		ret.Ret_InstallShard = &reply
		break
	}
}

type ShardKV_OperationDescriptor struct {
	ValidID                 int
	Val_PutAppend           *ShardKV_Action_Args_PutAppend
	Val_Get                 *ShardKV_Action_Args_Get
	Val_RequestConfigChange *ShardKV_Action_Args_RequestConfigChange
	Val_ReadState           *ShardKV_Action_Args_ReadState
	Val_PullShard           *ShardKV_Action_Args_PullShard
	Val_AckPullShard        *ShardKV_Action_Args_AckPullShard
	Val_InstallShard        *ShardKV_Action_Args_InstallShard
}
type ShardKV_ReturnValueDescriptor struct {
	ValidID                 int
	Ret_PutAppend           *ShardKV_Action_Reply_PutAppend
	Ret_Get                 *ShardKV_Action_Reply_Get
	Ret_RequestConfigChange *ShardKV_Action_Reply_RequestConfigChange
	Ret_ReadState           *ShardKV_Action_Reply_ReadState
	Ret_PullShard           *ShardKV_Action_Reply_PullShard
	Ret_AckPullShard        *ShardKV_Action_Reply_AckPullShard
	Ret_InstallShard        *ShardKV_Action_Reply_InstallShard
}

// Template below. No modification needed.

type ShardKV_Op struct {
	RealOp       ShardKV_OperationDescriptor
	ClientId     int64
	ClientSerial int64
}

type ShardKV_EventResponse struct {
	IsOk               bool
	IsAlreadyCommitted bool
	IsClearedRequest   bool
	IsKilled           bool
	Value              ShardKV_ReturnValueDescriptor
}

type ShardKV_EventOp struct {
	req *ShardKV_Op
	res chan ShardKV_EventResponse
}

type ShardKV_RSMState struct {
	State                      ShardKV_State
	CommittedIndicesDontAccess map[int64]int64
}

func (kv *ShardKV_RSMState) getCommittedIndex(clientId int64) int64 {
	val, ok := kv.CommittedIndicesDontAccess[clientId]
	if ok {
		return val
	} else {
		return -1
	}
}

func (kv *ShardKV_RSMState) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*kv)
	data := w.Bytes()
	return data
}

func ShardKV_deserializeKVStore(data []byte) *ShardKV_RSMState {
	newStore := ShardKV_RSMState{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&newStore) != nil {
		panic("Bad state")
	}
	return &newStore
}

type ShardKV_ApplyLogResult struct {
	status string
	value  ShardKV_ReturnValueDescriptor
}

// this is a pure function.
// deterministic state transfer here.
func (kv *ShardKV_RSMState) applyLog(log *ShardKV_Op) ShardKV_ApplyLogResult {
	result := ShardKV_ApplyLogResult{
		status: "Ok",
		value:  ShardKV_ReturnValueDescriptor{},
	}
	if log.ClientSerial <= kv.getCommittedIndex(log.ClientId) {
		// already committed
		result.status = "Outdated"
		return result
	}
	kv.PerformActions(&log.RealOp, &result.value)
	/* ShardKV_ApplyLogOperation_Begin */
	/* ShardKV_ApplyLogOperation_End */
	kv.CommittedIndicesDontAccess[log.ClientId] = log.ClientSerial
	return result
}

type ShardKV_ClientReq struct {
	ClientId     int64
	ClientSerial int64
}
type ShardKV_ReplicatedStateMachine struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	rwdead  sync.RWMutex
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore         *ShardKV_RSMState
	eventQueue      chan ShardKV_EventOp
	killChannel     chan int
	lastSeenTerm    int
	pendingRequests map[ShardKV_ClientReq]chan ShardKV_EventResponse
	persister       *raft.Persister
	gid             int
}

const (
	ShardKV_TraceEnabled       = false
	ShardKV_ClientTraceEnabled = false
)

func (kv *ShardKV_ReplicatedStateMachine) tracef(msg string, args ...interface{}) {
	if ShardKV_TraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[SKvS][%d-%d-%d-%d][%d-%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), kv.gid, kv.me, m)
	}
}

func (kv *ShardKV_ReplicatedStateMachine) pushEvent(ev ShardKV_EventOp) bool {
	kv.rwdead.RLock()
	flag := false
	if !kv.killed() {
		kv.eventQueue <- ev
		flag = true
	}
	kv.rwdead.RUnlock()
	return flag
}

type ShardKV_ActionArgs struct {
	RealOp       ShardKV_OperationDescriptor
	ClientId     int64
	ClientSerial int64
}
type ShardKV_ActionReply struct {
	Value ShardKV_ReturnValueDescriptor
	Err   Err
}

func (kv *ShardKV_ReplicatedStateMachine) ShardKV_Action(args *ShardKV_ActionArgs, reply *ShardKV_ActionReply) {
	//kv.tracef("Get Req %+v", *args)
	req := ShardKV_Op{
		RealOp:       args.RealOp,
		ClientId:     args.ClientId,
		ClientSerial: args.ClientSerial,
	}
	responseChannel := make(chan ShardKV_EventResponse, 1)
	event := ShardKV_EventOp{
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

func (kv *ShardKV_ReplicatedStateMachine) Kill() {
	kv.rwdead.Lock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killChannel <- 114514
	kv.rwdead.Unlock()
	// Your code here, if desired.
}

func (kv *ShardKV_ReplicatedStateMachine) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV_ReplicatedStateMachine) performRaft(log *ShardKV_Op) (bool, bool, int) {
	index, observedTerm, isLeader := kv.rf.Start(*log)
	termUpdated := observedTerm > kv.lastSeenTerm
	//kv.tracef("Term check (raft) %d->%d", kv.lastSeenTerm, observedTerm)
	if observedTerm > kv.lastSeenTerm {
		kv.lastSeenTerm = observedTerm
	}
	return isLeader, termUpdated, index
}

// Returns OK, ErrWrongLeader or ErrOutdatedGet.
func (kv *ShardKV_ReplicatedStateMachine) tryPerformRaftAndClearup(log *ShardKV_Op, res chan ShardKV_EventResponse) (bool, int) {
	if kv.kvstore.getCommittedIndex(log.ClientId) >= log.ClientSerial {
		res <- ShardKV_EventResponse{IsAlreadyCommitted: true}
	}
	reqID := ShardKV_ClientReq{ClientId: log.ClientId, ClientSerial: log.ClientSerial}
	val, ok := kv.pendingRequests[reqID]
	if ok {
		val <- ShardKV_EventResponse{IsClearedRequest: true}
		delete(kv.pendingRequests, reqID)
	}
	isLeader, termUpdated, index := kv.performRaft(log)
	if termUpdated {
		kv.clearPendingRequestsByTermChange()
	}
	if !isLeader {
		//kv.tracef("Not leader")
		res <- ShardKV_EventResponse{}
		return false, 0
	}
	kv.tracef("A new request %+v", *log)

	kv.pendingRequests[reqID] = res
	return true, index
}

func (kv *ShardKV_ReplicatedStateMachine) clearPendingRequestsByTermChange() {
	m := kv.pendingRequests
	for _, ch := range m {
		ch <- ShardKV_EventResponse{}
	}
	kv.pendingRequests = make(map[ShardKV_ClientReq]chan ShardKV_EventResponse)
}
func (kv *ShardKV_ReplicatedStateMachine) printAllPendingRequests(term int) {
	keys := make([]ShardKV_ClientReq, 0, len(kv.pendingRequests))
	for k := range kv.pendingRequests {
		keys = append(keys, k)
	}
	kv.tracef("Pending requests for term %d: %+v", term, keys)
}
func (kv *ShardKV_ReplicatedStateMachine) eventLoop() {
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
				case ShardKV_Op:
					if !(msg.CommandValid && (!msg.SnapshotValid)) {
						log.Panic("Bad state")
					}
					kv.tracef("Applying op %+v", op)
					ret := kv.kvstore.applyLog(&op)
					kv.tracef("Applied op %+v = %+v", op, ret)
					reqID := ShardKV_ClientReq{ClientId: op.ClientId, ClientSerial: op.ClientSerial}
					val, ok := kv.pendingRequests[reqID]
					if ok {
						if ret.status == "Outdated" {
							val <- ShardKV_EventResponse{IsAlreadyCommitted: true}
						} else {
							val <- ShardKV_EventResponse{IsOk: true, Value: ret.value}
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
					kv.kvstore = ShardKV_deserializeKVStore(msg.Snapshot)
				}
			}
			newTerm, _ := kv.rf.GetState()
			//kv.tracef("Term check (apply) %d->%d", kv.lastSeenTerm, newTerm)
			if newTerm > kv.lastSeenTerm {
				kv.lastSeenTerm = newTerm
				kv.clearPendingRequestsByTermChange()
			} else {
				// in either case, remove overcommitted request.
				newPendingRequests := make(map[ShardKV_ClientReq]chan ShardKV_EventResponse)
				// remove overcommitted request
				for reqID, ch := range kv.pendingRequests {
					if kv.kvstore.getCommittedIndex(reqID.ClientId) >= reqID.ClientSerial {
						ch <- ShardKV_EventResponse{IsAlreadyCommitted: true}
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
			remainEv.res <- ShardKV_EventResponse{IsKilled: true}
		default:
			break GRACEFUL_SHUTDOWN
		}
	}
	for _, pending := range kv.pendingRequests {
		pending <- ShardKV_EventResponse{IsKilled: true}
	}
}

func ShardKV_StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int) *ShardKV_ReplicatedStateMachine {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(ShardKV_Op{})

	kv := new(ShardKV_ReplicatedStateMachine)
	kv.me = me
	kv.gid = gid
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	snapshot := kv.persister.ReadSnapshot()

	if snapshot != nil && len(snapshot) != 0 {
		kv.kvstore = ShardKV_deserializeKVStore(snapshot)
	} else {
		kv.kvstore = &ShardKV_RSMState{State: New_ShardKV_State(gid),
			CommittedIndicesDontAccess: make(map[int64]int64)}
	}

	kv.eventQueue = make(chan ShardKV_EventOp)
	kv.killChannel = make(chan int, 1)
	kv.lastSeenTerm = -1
	kv.pendingRequests = make(map[ShardKV_ClientReq]chan ShardKV_EventResponse)
	kv.dead = 0

	// You may need initialization code here.
	go kv.eventLoop()
	return kv
}

type ShardKV_Clerk struct {
	servers                                     []*labrpc.ClientEnd
	lastLeader                                  int
	clientId                                    int64
	clientSerial                                int64
	lockSoThatClientWillNotSendMultipleRequests sync.Mutex
	// You will have to modify this struct.
}

func (ck *ShardKV_Clerk) getNextSerial() int64 {
	serial := ck.clientSerial
	ck.clientSerial++
	return serial
}

func ShardKV_MakeClerk(servers []*labrpc.ClientEnd) *ShardKV_Clerk {
	ck := new(ShardKV_Clerk)
	ck.servers = servers
	ck.clientSerial = 0
	ck.clientId = nrand()
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

func (ck *ShardKV_Clerk) tracef(msg string, args ...interface{}) {
	if ShardKV_ClientTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[SKvC][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), ck.clientId, m)
	}
}

func (ck *ShardKV_Clerk) PerformGeneralAction(op ShardKV_OperationDescriptor, failOnWrongLeader, failOnKilled, failOnBadSend bool) (*ShardKV_ReturnValueDescriptor, string) {
	ck.lockSoThatClientWillNotSendMultipleRequests.Lock()
	defer ck.lockSoThatClientWillNotSendMultipleRequests.Unlock()
	leader := ck.lastLeader
	args := ShardKV_ActionArgs{RealOp: op, ClientId: ck.clientId, ClientSerial: ck.clientSerial}
	reply := ShardKV_ActionReply{}
	ck.tracef("Sending request %+v", args)
OP_LOOP:
	for {
		ck.tracef("Trying server %d", leader)
		ok := ck.servers[leader].Call("ShardKV.ShardKV_Action", &args, &reply)
		ck.tracef("Req responded %d", leader)
		if !ok {
			ck.tracef("Sending request %+v failed.", args)
			if failOnBadSend {
				return nil, ErrBadSend
			}
			reply = ShardKV_ActionReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
			continue // try again
		}
		switch reply.Err {
		case OK:
			ck.tracef("OK")
			ck.clientSerial++
			break OP_LOOP
		case ErrKilled:
			ck.tracef("Killed")
			if failOnKilled {
				return nil, ErrKilled
			}
			reply = ShardKV_ActionReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrWrongLeader:
			ck.tracef("Wrong leader")
			if failOnWrongLeader {
				return nil, ErrWrongLeader
			}
			reply = ShardKV_ActionReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrOutdatedOp:
			ck.tracef("Outdated serial %d", ck.clientSerial)
			ck.lastLeader = leader
			ck.clientSerial++
			return nil, ErrOutdatedOp
		case ErrButItIsYouWhoTimedoutFirst:
			log.Panicln("This should not happen.")
		default:
			log.Panicf("This (%s) should not happen.", reply.Err)
		}
	}

	ck.lastLeader = leader
	val := reply.Value
	return &val, OK
}
