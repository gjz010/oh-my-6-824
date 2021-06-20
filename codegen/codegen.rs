struct ActionDef {
    name: String,
    argsDef: String,
    replyDef: String,
    actionPerform: String,
}

impl ActionDef {
    fn generateArgsAndReply(&self, rsmName: &str) -> String {
        return format!(
            r#"type {}_Action_Args_{} struct {{
    {}
}}
type {}_Action_Reply_{} struct {{
    {}
}}"#,
            rsmName, self.name, self.argsDef, rsmName, self.name, self.replyDef
        );
    }
    fn generatePerformActionsBranch(&self, rsmName: &str, id: usize) -> String {
        return format!(
            r#"case {}:
    reply := kv.State.TransferAction_{}(*op.Val_{})
    ret.Ret_{} = &reply
    break"#,
            id, self.name, self.name, self.name
        );
    }
    fn generateOperationDescriptorBranch(&self, rsmName: &str) -> String {
        return format!(
            r#"Val_{} *{}_Action_Args_{}"#,
            self.name, rsmName, self.name
        );
    }
    fn generateReturnValueDescriptorBranch(&self, rsmName: &str) -> String {
        return format!(
            r#"Ret_{} *{}_Action_Reply_{}"#,
            self.name, rsmName, self.name
        );
    }
    fn generateTransferFunction(&self, rsmName: &str) -> String {
        return format!(
            r#"func (state *{}_State) TransferAction_{}(args {}_Action_Args_{}) {}_Action_Reply_{} {{
	{}
}}"#,
            rsmName, self.name, rsmName, self.name, rsmName, self.name, self.actionPerform
        );
    }
    fn generateClerkTypedStub(&self, rsmName: &str, id: usize) -> String {
        return format!(
            r#"func (ck *{}_Clerk) {}_Action_{}(op {}_Action_Args_{}) (*{}_Action_Reply_{}, bool) {{
	descriptor := {}_OperationDescriptor{{
		ValidID: {},
		Val_ID:  &op,
	}}
	ret, err := ck.PerformGeneralAction(descriptor)
	if err {{
		return nil, true
	}}
	if ret.ValidID != {} {{
		log.Panicln("return type check failed.")
	}}
	return ret.Ret_{}, false
}}"#,
            rsmName,
            rsmName,
            self.name,
            rsmName,
            self.name,
            rsmName,
            self.name,
            rsmName,
            id,
            id,
            self.name
        );
    }
}
struct RSMDef {
    name: String,
    shortNameServer: String,
    shortNameClient: String,
    stateDef: String,
    stateInitializer: String,
    actions: Vec<ActionDef>,
}
impl RSMDef {
    fn generateStateDef(&self) -> String {
        format!(
            r#"type {}_State struct {{
    {}
}}

func New_{}_State() {}_State {{
	return {}_State{{
	{}
	}}
}}"#,
            self.name, self.stateDef, self.name, self.name, self.name, self.stateInitializer
        )
    }
    fn generateActions(&self) -> String {
        // PerformActions
        let head = format!(
            r#"func (kv *{}_RSMState) PerformActions(op *{}_OperationDescriptor, ret *{}_ReturnValueDescriptor) {{
	ret.ValidID = op.ValidID
	switch op.ValidID {{"#,
            self.name, self.name, self.name
        );
        let tail = r#"	}
}"#;
        let branches = self
            .actions
            .iter()
            .enumerate()
            .map(|(i, x)| x.generatePerformActionsBranch(&self.name, i + 1))
            .collect::<Vec<String>>()
            .join("\n");
        let perform_actions = format!("{}\n{}\n{}", head, branches, tail);
        // OD & RVD structs
        let head = format!(
            r#"type {}_OperationDescriptor struct {{
	ValidID int"#,
            self.name
        );
        let tail = "}";
        let branches = self
            .actions
            .iter()
            .map(|x| x.generateOperationDescriptorBranch(&self.name))
            .collect::<Vec<String>>()
            .join("\n");
        let od = format!("{}\n{}\n{}", head, branches, tail);
        let head = format!(
            r#"type {}_ReturnValueDescriptor struct {{
	ValidID int"#,
            self.name
        );
        let tail = "}";
        let branches = self
            .actions
            .iter()
            .map(|x| x.generateReturnValueDescriptorBranch(&self.name))
            .collect::<Vec<String>>()
            .join("\n");
        let rvd = format!("{}\n{}\n{}", head, branches, tail);
        let transfer_actions = self
            .actions
            .iter()
            .map(|x| x.generateTransferFunction(&self.name))
            .collect::<Vec<String>>()
            .join("\n");
        let args_and_replies = self
            .actions
            .iter()
            .map(|x| x.generateArgsAndReply(&self.name))
            .collect::<Vec<String>>()
            .join("\n");
        format!(
            "{}\n{}\n{}\n{}\n{}",
            transfer_actions, args_and_replies, perform_actions, od, rvd
        )
    }
    fn replaceBody(&self) -> String {
        let mut body = GO_TEMPLATE_BODY.to_string();
        let replace_rules = vec![
            ("____DEUSEXMACHINA_ShortName_Server", &self.shortNameServer),
            ("____DEUSEXMACHINA_ShortName_Clerk", &self.shortNameClient),
            ("____DEUSEXMACHINA", &self.name),
        ];
        for (a, b) in replace_rules.iter() {
            body = body.replace(a, b);
        }
        body
    }
    fn generateRSM(&self) -> String {
        format!(
            "{}\n{}\n{}\n{}\n",
            GO_TEMPLATE_HEADER,
            self.generateActions(),
            self.generateStateDef(),
            self.replaceBody()
        )
    }
}

fn main() {
    /*
    let kvstore_def = RSMDef{
        name: "KVStore".into(),
        shortNameServer: "KvSt".into(),
        shortNameClient: "KvCl".into(),
        stateDef: "Store map[string]string".into(),
        stateInitializer: "Store: make(map[string]string),".into(),
        actions: vec![]
    };

    println!("{}", kvstore_def.generateRSM());
    */
    let shard_controller_def = RSMDef {
        name: "ShardController".into(),
        shortNameServer: "Shrd".into(),
        shortNameClient: "SCCl".into(),
        stateDef: "ConfigVersions []Config".into(),
        stateInitializer: "ConfigVersions: make([]Config,0),".into(),
        actions: vec![
            ActionDef {
                name: "Join".into(),
                argsDef: "Servers map[int][]string".into(),
                replyDef: "Magic int".into(),
                actionPerform: "/* TODO: Implement Join */".into(),
            },
            ActionDef {
                name: "Leave".into(),
                argsDef: "GIDs []int".into(),
                replyDef: "Magic int".into(),
                actionPerform: "/* TODO: Implement Leave */".into(),
            },
            ActionDef {
                name: "Move".into(),
                argsDef: "Shard int\nGID int".into(),
                replyDef: "Magic int".into(),
                actionPerform: "/* TODO: Implement Move */".into(),
            },
            ActionDef {
                name: "Query".into(),
                argsDef: "Num int".into(),
                replyDef: "Config Config".into(),
                actionPerform: "/* TODO: Implement Query */".into(),
            },
        ],
    };
    println!("{}", shard_controller_def.generateRSM());
}

// Template.
const GO_TEMPLATE_HEADER: &str = r#"
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
)"#;

/*
func (kv *____DEUSEXMACHINA_RSMState) PerformActions(op *____DEUSEXMACHINA_OperationDescriptor, ret *____DEUSEXMACHINA_ReturnValueDescriptor) {
    ret.ValidID = op.ValidID
    switch op.ValidID {
    // Branches generation
    case ____DEUSEXMACHINA_ID:
        reply := kv.State.TransferAction_ID(*op.Val_ID)
        ret.Ret_ID = &reply
        break
    }
}

type ____DEUSEXMACHINA_OperationDescriptor struct {
    ValidID int
    Val_ID  *____DEUSEXMACHINA_Action_Args_ID
}
type ____DEUSEXMACHINA_ReturnValueDescriptor struct {
    ValidID int
    Ret_ID  *____DEUSEXMACHINA_Action_Reply_ID
}

/*CODEGEN_SEP*/
const ____DEUSEXMACHINA_ID = 0

type ____DEUSEXMACHINA_Action_Args_ID struct {
}
type ____DEUSEXMACHINA_Action_Reply_ID struct {
}

func (state *____DEUSEXMACHINA_State) TransferAction_ID(____DEUSEXMACHINA_Action_Args_ID) ____DEUSEXMACHINA_Action_Reply_ID {
    return ____DEUSEXMACHINA_Action_Reply_ID{}
}

func (ck *____DEUSEXMACHINA_Clerk) ____DEUSEXMACHINA_Action_ID(op ____DEUSEXMACHINA_Action_Args_ID) (*____DEUSEXMACHINA_Action_Reply_ID, bool) {
    descriptor := ____DEUSEXMACHINA_OperationDescriptor{
        ValidID: ____DEUSEXMACHINA_ID,
        Val_ID:  &op,
    }
    ret, err := ck.PerformGeneralAction(descriptor)
    if err {
        return nil, true
    }
    if ret.ValidID != ____DEUSEXMACHINA_ID {
        log.Panicln("return type check failed.")
    }
    return ret.Ret_ID, false
}

// These need to be generated for every RSM.

type ____DEUSEXMACHINA_State struct {
}

func New_____DEUSEXMACHINA_State() ____DEUSEXMACHINA_State {
    return ____DEUSEXMACHINA_State{}
}
*/
const GO_TEMPLATE_BODY: &str = r#"
// Template below. No modification needed.

type ____DEUSEXMACHINA_Op struct {
	RealOp       ____DEUSEXMACHINA_OperationDescriptor
	ClientId     int64
	ClientSerial int64
}

type ____DEUSEXMACHINA_EventResponse struct {
	IsOk               bool
	IsAlreadyCommitted bool
	IsClearedRequest   bool
	IsKilled           bool
	Value              ____DEUSEXMACHINA_ReturnValueDescriptor
}

type ____DEUSEXMACHINA_EventOp struct {
	req *____DEUSEXMACHINA_Op
	res chan ____DEUSEXMACHINA_EventResponse
}

type ____DEUSEXMACHINA_RSMState struct {
	State                      ____DEUSEXMACHINA_State
	CommittedIndicesDontAccess map[int64]int64
}

func (kv *____DEUSEXMACHINA_RSMState) getCommittedIndex(clientId int64) int64 {
	val, ok := kv.CommittedIndicesDontAccess[clientId]
	if ok {
		return val
	} else {
		return -1
	}
}

func (kv *____DEUSEXMACHINA_RSMState) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(*kv)
	data := w.Bytes()
	return data
}

func ____DEUSEXMACHINA_deserializeKVStore(data []byte) *____DEUSEXMACHINA_RSMState {
	newStore := ____DEUSEXMACHINA_RSMState{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&newStore) != nil {
		panic("Bad state")
	}
	return &newStore
}

type ____DEUSEXMACHINA_ApplyLogResult struct {
	status string
	value  ____DEUSEXMACHINA_ReturnValueDescriptor
}

// this is a pure function.
// deterministic state transfer here.
func (kv *____DEUSEXMACHINA_RSMState) applyLog(log *____DEUSEXMACHINA_Op) ____DEUSEXMACHINA_ApplyLogResult {
	result := ____DEUSEXMACHINA_ApplyLogResult{
		status: "Ok",
		value:  ____DEUSEXMACHINA_ReturnValueDescriptor{},
	}
	if log.ClientSerial <= kv.getCommittedIndex(log.ClientId) {
		// already committed
		result.status = "Outdated"
		return result
	}
	kv.PerformActions(&log.RealOp, &result.value)
	/* ____DEUSEXMACHINA_ApplyLogOperation_Begin */
	/* ____DEUSEXMACHINA_ApplyLogOperation_End */
	kv.CommittedIndicesDontAccess[log.ClientId] = log.ClientSerial
	return result
}

type ____DEUSEXMACHINA_ClientReq struct {
	ClientId     int64
	ClientSerial int64
}
type ____DEUSEXMACHINA_ReplicatedStateMachine struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	rwdead  sync.RWMutex
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore         *____DEUSEXMACHINA_RSMState
	eventQueue      chan ____DEUSEXMACHINA_EventOp
	killChannel     chan int
	lastSeenTerm    int
	pendingRequests map[____DEUSEXMACHINA_ClientReq]chan ____DEUSEXMACHINA_EventResponse
	persister       *raft.Persister
}

const (
	____DEUSEXMACHINA_TraceEnabled       = true
	____DEUSEXMACHINA_ClientTraceEnabled = false
)

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) tracef(msg string, args ...interface{}) {
	if ____DEUSEXMACHINA_TraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[____DEUSEXMACHINA_ShortName_Server][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), kv.me, m)
	}
}

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) pushEvent(ev ____DEUSEXMACHINA_EventOp) bool {
	kv.rwdead.RLock()
	flag := false
	if !kv.killed() {
		kv.eventQueue <- ev
		flag = true
	}
	kv.rwdead.RUnlock()
	return flag
}

type ____DEUSEXMACHINA_ActionArgs struct {
	RealOp       ____DEUSEXMACHINA_OperationDescriptor
	ClientId     int64
	ClientSerial int64
}
type ____DEUSEXMACHINA_ActionReply struct {
	Value ____DEUSEXMACHINA_ReturnValueDescriptor
	Err   Err
}

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) ____DEUSEXMACHINA_Action(args *____DEUSEXMACHINA_ActionArgs, reply *____DEUSEXMACHINA_ActionReply) {
	//kv.tracef("Get Req %+v", *args)
	req := ____DEUSEXMACHINA_Op{
		RealOp:       args.RealOp,
		ClientId:     args.ClientId,
		ClientSerial: args.ClientSerial,
	}
	responseChannel := make(chan ____DEUSEXMACHINA_EventResponse, 1)
	event := ____DEUSEXMACHINA_EventOp{
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

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) Kill() {
	kv.rwdead.Lock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.killChannel <- 114514
	kv.rwdead.Unlock()
	// Your code here, if desired.
}

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) performRaft(log *____DEUSEXMACHINA_Op) (bool, bool, int) {
	index, observedTerm, isLeader := kv.rf.Start(*log)
	termUpdated := observedTerm > kv.lastSeenTerm
	//kv.tracef("Term check (raft) %d->%d", kv.lastSeenTerm, observedTerm)
	if observedTerm > kv.lastSeenTerm {
		kv.lastSeenTerm = observedTerm
	}
	return isLeader, termUpdated, index
}

// Returns OK, ErrWrongLeader or ErrOutdatedGet.
func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) tryPerformRaftAndClearup(log *____DEUSEXMACHINA_Op, res chan ____DEUSEXMACHINA_EventResponse) (bool, int) {
	if kv.kvstore.getCommittedIndex(log.ClientId) >= log.ClientSerial {
		res <- ____DEUSEXMACHINA_EventResponse{IsAlreadyCommitted: true}
	}
	reqID := ____DEUSEXMACHINA_ClientReq{ClientId: log.ClientId, ClientSerial: log.ClientSerial}
	val, ok := kv.pendingRequests[reqID]
	if ok {
		val <- ____DEUSEXMACHINA_EventResponse{IsClearedRequest: true}
		delete(kv.pendingRequests, reqID)
	}
	isLeader, termUpdated, index := kv.performRaft(log)
	if termUpdated {
		kv.clearPendingRequestsByTermChange()
	}
	if !isLeader {
		//kv.tracef("Not leader")
		res <- ____DEUSEXMACHINA_EventResponse{}
		return false, 0
	}
	kv.tracef("A new request %+v", *log)

	kv.pendingRequests[reqID] = res
	return true, index
}

func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) clearPendingRequestsByTermChange() {
	m := kv.pendingRequests
	for _, ch := range m {
		ch <- ____DEUSEXMACHINA_EventResponse{}
	}
	kv.pendingRequests = make(map[____DEUSEXMACHINA_ClientReq]chan ____DEUSEXMACHINA_EventResponse)
}
func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) printAllPendingRequests(term int) {
	keys := make([]____DEUSEXMACHINA_ClientReq, 0, len(kv.pendingRequests))
	for k := range kv.pendingRequests {
		keys = append(keys, k)
	}
	kv.tracef("Pending requests for term %d: %+v", term, keys)
}
func (kv *____DEUSEXMACHINA_ReplicatedStateMachine) eventLoop() {
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
				case ____DEUSEXMACHINA_Op:
					if !(msg.CommandValid && (!msg.SnapshotValid)) {
						log.Panic("Bad state")
					}

					ret := kv.kvstore.applyLog(&op)
					kv.tracef("Applying op %+v = %+v", op, ret)
					reqID := ____DEUSEXMACHINA_ClientReq{ClientId: op.ClientId, ClientSerial: op.ClientSerial}
					val, ok := kv.pendingRequests[reqID]
					if ok {
						if ret.status == "Outdated" {
							val <- ____DEUSEXMACHINA_EventResponse{IsAlreadyCommitted: true}
						} else {
							val <- ____DEUSEXMACHINA_EventResponse{IsOk: true, Value: ret.value}
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
					kv.kvstore = ____DEUSEXMACHINA_deserializeKVStore(msg.Snapshot)
				}
			}
			newTerm, _ := kv.rf.GetState()
			//kv.tracef("Term check (apply) %d->%d", kv.lastSeenTerm, newTerm)
			if newTerm > kv.lastSeenTerm {
				kv.lastSeenTerm = newTerm
				kv.clearPendingRequestsByTermChange()
			} else {
				// in either case, remove overcommitted request.
				newPendingRequests := make(map[____DEUSEXMACHINA_ClientReq]chan ____DEUSEXMACHINA_EventResponse)
				// remove overcommitted request
				for reqID, ch := range kv.pendingRequests {
					if kv.kvstore.getCommittedIndex(reqID.ClientId) >= reqID.ClientSerial {
						ch <- ____DEUSEXMACHINA_EventResponse{IsAlreadyCommitted: true}
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
			remainEv.res <- ____DEUSEXMACHINA_EventResponse{IsKilled: true}
		default:
			break GRACEFUL_SHUTDOWN
		}
	}
	for _, pending := range kv.pendingRequests {
		pending <- ____DEUSEXMACHINA_EventResponse{IsKilled: true}
	}
}

func ____DEUSEXMACHINA_StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *____DEUSEXMACHINA_ReplicatedStateMachine {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(____DEUSEXMACHINA_Op{})

	kv := new(____DEUSEXMACHINA_ReplicatedStateMachine)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	snapshot := kv.persister.ReadSnapshot()

	if snapshot != nil && len(snapshot) != 0 {
		kv.kvstore = ____DEUSEXMACHINA_deserializeKVStore(snapshot)
	} else {
		kv.kvstore = &____DEUSEXMACHINA_RSMState{State: New_____DEUSEXMACHINA_State(),
			CommittedIndicesDontAccess: make(map[int64]int64)}
	}

	kv.eventQueue = make(chan ____DEUSEXMACHINA_EventOp)
	kv.killChannel = make(chan int, 1)
	kv.lastSeenTerm = -1
	kv.pendingRequests = make(map[____DEUSEXMACHINA_ClientReq]chan ____DEUSEXMACHINA_EventResponse)
	kv.dead = 0

	// You may need initialization code here.
	go kv.eventLoop()
	return kv
}

type ____DEUSEXMACHINA_Clerk struct {
	servers                                     []*labrpc.ClientEnd
	lastLeader                                  int
	clientId                                    int64
	clientSerial                                int64
	lockSoThatClientWillNotSendMultipleRequests sync.Mutex
	// You will have to modify this struct.
}

func (ck *____DEUSEXMACHINA_Clerk) getNextSerial() int64 {
	serial := ck.clientSerial
	ck.clientSerial++
	return serial
}

func ____DEUSEXMACHINA_MakeClerk(servers []*labrpc.ClientEnd) *____DEUSEXMACHINA_Clerk {
	ck := new(____DEUSEXMACHINA_Clerk)
	ck.servers = servers
	ck.clientSerial = 0
	ck.clientId = nrand()
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

func (ck *____DEUSEXMACHINA_Clerk) tracef(msg string, args ...interface{}) {
	if ____DEUSEXMACHINA_ClientTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[____DEUSEXMACHINA_ShortName_Clerk][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), ck.clientId, m)
	}
}

func (ck *____DEUSEXMACHINA_Clerk) PerformGeneralAction(op ____DEUSEXMACHINA_OperationDescriptor) (*____DEUSEXMACHINA_ReturnValueDescriptor, bool) {
	ck.lockSoThatClientWillNotSendMultipleRequests.Lock()
	defer ck.lockSoThatClientWillNotSendMultipleRequests.Unlock()
	leader := ck.lastLeader
	args := ____DEUSEXMACHINA_ActionArgs{RealOp: op, ClientId: ck.clientId, ClientSerial: ck.getNextSerial()}
	reply := ____DEUSEXMACHINA_ActionReply{}
	ck.tracef("Sending request %+v", args)
OP_LOOP:
	for {
		ck.tracef("Trying server %d", leader)
		ok := ck.servers[leader].Call("____DEUSEXMACHINA_ReplicatedStateMachine.____DEUSEXMACHINA_Action", &args, &reply)
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
			reply = ____DEUSEXMACHINA_ActionReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrWrongLeader:
			ck.tracef("Wrong leader")
			reply = ____DEUSEXMACHINA_ActionReply{}
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
"#;
