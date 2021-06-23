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

const (
	NoShardData_TemporaryFail = "TemporaryFail"
	NoShardData_WrongGroup    = "WrongGroup"
	OpPut                     = "Put"
	OpAppend                  = "Append"
)

type ShardKV_State struct {
	Shards         map[int]*ShardData
	CurrentVersion int
	CurrentConfig  shardctrler.Config
	PreviousConfig shardctrler.Config
	Gid            int
	ShardVersions  map[int]int
}

func (shard *ShardData) Clone() *ShardData {
	var newshard ShardData
	newshard.Store = make(map[string]string)
	for k, v := range shard.Store {
		newshard.Store[k] = v
	}
	newshard.CommittedIndices = make(map[int64]int64)
	for k, v := range shard.CommittedIndices {
		newshard.CommittedIndices[k] = v
	}
	return &newshard

}

func New_ShardKV_State(gid int) ShardKV_State {
	return ShardKV_State{
		Shards:         make(map[int]*ShardData),
		CurrentVersion: 0,
		CurrentConfig:  shardctrler.InitialConfig()[0],
		Gid:            gid,
		ShardVersions:  make(map[int]int),
	}
}

type ShardData struct {
	Store            map[string]string
	CommittedIndices map[int64]int64
}

func (state *ShardKV_State) TryGetShardData(shard int) (*ShardData, string) {
	if state.CurrentConfig.Shards[shard] != state.Gid {
		return nil, NoShardData_WrongGroup
	}
	if state.ShardVersions[shard] < state.CurrentVersion {
		return nil, NoShardData_TemporaryFail
	}
	shardData := state.Shards[shard]
	if shardData == nil {
		return nil, NoShardData_TemporaryFail
	}
	return shardData, OK
}
func (state *ShardKV_State) TransferAction_PutAppend_Impl(args ShardKV_Action_Args_PutAppend) ShardKV_Action_Reply_PutAppend {
	shard, err := state.TryGetShardData(key2shard(args.Key))
	if err != OK {
		if err == NoShardData_WrongGroup {
			return ShardKV_Action_Reply_PutAppend{WrongGroup: true}
		}
		if err == NoShardData_TemporaryFail {
			return ShardKV_Action_Reply_PutAppend{TryAgainLater: true}
		}
	}
	if shard.CommittedIndices[args.ClientId] >= args.ClientSerial {
		return ShardKV_Action_Reply_PutAppend{OutDated: true}
	}
	if args.Op == OpPut {
		shard.Store[args.Key] = args.Value
	} else {
		shard.Store[args.Key] = shard.Store[args.Key] + args.Value
	}
	shard.CommittedIndices[args.ClientId] = args.ClientSerial
	if TransferLogEnabled {
		fmt.Printf("Writing key %s into shard %d = %s", args.Key, key2shard(args.Key), shard.Store[args.Key])
	}

	return ShardKV_Action_Reply_PutAppend{Pong: shard.Store[args.Key]}
}
func (state *ShardKV_State) TransferAction_Get_Impl(args ShardKV_Action_Args_Get) ShardKV_Action_Reply_Get {
	shard, err := state.TryGetShardData(key2shard(args.Key))
	if err != OK {
		if err == NoShardData_WrongGroup {
			return ShardKV_Action_Reply_Get{WrongGroup: true}
		}
		if err == NoShardData_TemporaryFail {
			return ShardKV_Action_Reply_Get{TryAgainLater: true}
		}
	}
	if shard.CommittedIndices[args.ClientId] >= args.ClientSerial {
		return ShardKV_Action_Reply_Get{OutDated: true}
	}
	val := shard.Store[args.Key]
	shard.CommittedIndices[args.ClientId] = args.ClientSerial
	if TransferLogEnabled {
		fmt.Printf("Reading key %s form shard %d = %s", args.Key, key2shard(args.Key), val)
	}
	return ShardKV_Action_Reply_Get{Value: val}
}
func (state *ShardKV_State) TransferAction_ReadState_Impl(args ShardKV_Action_Args_ReadState) ShardKV_Action_Reply_ReadState {
	config := state.CurrentConfig
	var missingShards []int
	var missingShardSources []int
	for shard, gid := range config.Shards {
		if gid == state.Gid {
			_, err := state.TryGetShardData(shard)
			if err != OK {
				missingShards = append(missingShards, shard)
				missingShardSources = append(missingShardSources, state.PreviousConfig.Shards[shard])
			}
		}
	}
	return ShardKV_Action_Reply_ReadState{
		MissingShards:       missingShards,
		MissingShardSources: missingShardSources,
		CurrentVersion:      state.CurrentVersion,
	}
}
func (state *ShardKV_State) TransferAction_RequestConfigChange_Impl(args ShardKV_Action_Args_RequestConfigChange) ShardKV_Action_Reply_RequestConfigChange {
	success := false // does not really matter
	if state.CurrentVersion+1 == args.NewVersion {
		state.PreviousConfig = state.CurrentConfig
		state.CurrentConfig = args.Config
		state.CurrentVersion = args.NewVersion
		success = true
		// create unassigned shards
		for shard, gid := range state.CurrentConfig.Shards {
			if gid == state.Gid && state.PreviousConfig.Shards[shard] == 0 {
				if TransferLogEnabled {
					fmt.Printf("Creating brand new shard %d in group %d\n", shard, gid)
				}
				state.Shards[shard] = &ShardData{
					Store:            make(map[string]string),
					CommittedIndices: make(map[int64]int64),
				}
				state.ShardVersions[shard] = state.CurrentVersion
			}
			if gid == state.Gid && state.ShardVersions[shard] == state.CurrentVersion-1 {
				state.ShardVersions[shard] = state.CurrentVersion
			}
		}
	}
	return ShardKV_Action_Reply_RequestConfigChange{CurrentVer: state.CurrentVersion, Success: success}
}
func (state *ShardKV_State) TransferAction_PullShard_Impl(args ShardKV_Action_Args_PullShard) ShardKV_Action_Reply_PullShard {
	// must wait after finishes current version.
	if state.CurrentVersion <= args.Version {
		return ShardKV_Action_Reply_PullShard{Success: false}
	}
	if state.ShardVersions[args.Shard] != args.Version {
		return ShardKV_Action_Reply_PullShard{Success: false}
	}
	shard := state.Shards[args.Shard]
	if TransferLogEnabled {
		log.Printf("Pushing shard %d version %d from gid %d: %+v", args.Shard, args.Version, state.Gid, shard)
	}
	return ShardKV_Action_Reply_PullShard{
		Success: true,
		Shard:   *shard.Clone(),
	}
}
func (state *ShardKV_State) TransferAction_AckPullShard_Impl(args ShardKV_Action_Args_AckPullShard) ShardKV_Action_Reply_AckPullShard {
	log.Panicf("Not implemented yet\n")
	return ShardKV_Action_Reply_AckPullShard{}
}
func (state *ShardKV_State) TransferAction_InstallShard_Impl(args ShardKV_Action_Args_InstallShard) ShardKV_Action_Reply_InstallShard {
	if state.ShardVersions[args.Shard] < args.Version {
		state.Shards[args.Shard] = args.Data.Clone()
		state.ShardVersions[args.Shard] = args.Version
		if TransferLogEnabled {
			log.Printf("Installing shard %d version %d: %+v", args.Shard, args.Version, args.Data)
		}
		return ShardKV_Action_Reply_InstallShard{Magic: 1}
	}
	return ShardKV_Action_Reply_InstallShard{Magic: 2}

}

type ShardKV struct {
	me       int
	rf       *raft.Raft
	make_end func(string) *labrpc.ClientEnd
	gid      int
	//ctrlers  []*labrpc.ClientEnd

	ctrlerClerk *shardctrler.Clerk
	peerClerk   map[int]*ShardKV_Clerk
	inner       *ShardKV_ReplicatedStateMachine
}

// forward RPC to generated code
func (kv *ShardKV) ShardKV_Action(args *ShardKV_ActionArgs, reply *ShardKV_ActionReply) {
	kv.inner.ShardKV_Action(args, reply)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.inner.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) createGroupClerk(gid int, servers []string) {
	if kv.peerClerk[gid] == nil {
		var endpoints []*labrpc.ClientEnd
		for _, server := range servers {
			endpoints = append(endpoints, kv.make_end(server))
		}
		kv.peerClerk[gid] = ShardKV_MakeClerk(endpoints)
	}

}

func (ck *ShardKV_Clerk) changeLeader() {
	ck.lastLeader++
	if ck.lastLeader >= len(ck.servers) {
		ck.lastLeader = 0
	}
}
func (kv *ShardKV) sidecar() {
	kv.Ttracef("Sidecar started.")
	selfClerk := kv.peerClerk[kv.gid]
	selfClerk.lastLeader = kv.me
RETRY:
	for !kv.inner.killed() {
		args := ShardKV_Action_Args_ReadState{Magic: 1}
		state, err := selfClerk.Action_ReadState(args, true, true, true)
		if err != OK {
			// not leader.
			time.Sleep(100 * time.Millisecond)
			continue RETRY
		}
		if len(state.MissingShards) > 0 {
			kv.Ttracef("Trying to fetch missing shards %+v", state.MissingShards)
			// try to fetch missing shards
			// first meet some new peers
			oldConfig := kv.ctrlerClerk.Query(state.CurrentVersion - 1)
			for gid, servers := range oldConfig.Groups {
				kv.createGroupClerk(gid, servers)
			}
			for id := range state.MissingShards {
				shard := state.MissingShards[id]
				source := state.MissingShardSources[id]
				var data *ShardData
			FETCH_SHARD:
				for {
					pullArgs := ShardKV_Action_Args_PullShard{
						Version: state.CurrentVersion - 1,
						Shard:   shard,
					}
					if source == 0 {
						log.Panicln("Bad source 0!")
					}
					if kv.peerClerk[source] == nil {
						log.Panicf("Unrecognized source %d!", source)
					}
					kv.Ttracef("Trying to pull shard %d(v=%d) from group %d leader %d", pullArgs.Shard, pullArgs.Version, source, kv.peerClerk[source].lastLeader)
					edata, err := kv.peerClerk[source].Action_PullShard(pullArgs, false, true, true)

					if err != OK {
						if err == ErrOutdatedOp {
							continue FETCH_SHARD
						}
						oldLeader := kv.peerClerk[source].lastLeader
						if err == ErrBadSend || err == ErrKilled {
							if kv.inner.killed() {
								return
							} else {
								kv.peerClerk[source].changeLeader()
							}
						}
						kv.Ttracef("Failed pulling shard %d(v=%d) from group %d leader %d because %s", pullArgs.Shard, pullArgs.Version, source, oldLeader, err)
						continue FETCH_SHARD
					}
					if !edata.Success {
						time.Sleep(100 * time.Millisecond)
						continue FETCH_SHARD
					}

					data = &edata.Shard
					kv.Ttracef("Pulled shard %d from %d: %+v", shard, source, edata)
					break
					// install the shard.
				}

				//INSTALL_SHARD:
				for {
					if kv.inner.killed() {
						return
					}
					installArgs := ShardKV_Action_Args_InstallShard{
						Version: state.CurrentVersion,
						Shard:   shard,
						Data:    *data,
					}
					ret, err := selfClerk.Action_InstallShard(installArgs, true, true, true)
					if err != OK {
						if err == ErrOutdatedOp {
							kv.Ttracef("Installed shard %d", shard)
							break
						}
						if err == ErrBadSend || err == ErrKilled {
							if kv.inner.killed() {
								return
							} else {
								kv.peerClerk[source].changeLeader()
							}
						}
						if err == ErrWrongLeader {
							selfClerk.clientSerial++
						}
						continue RETRY
					}
					kv.Ttracef("Installed shard %d version %d (%d)", shard, state.CurrentVersion, ret.Magic)
					break
				}
			}
			continue RETRY
		} else {
			// check new version
			config := kv.ctrlerClerk.Query(state.CurrentVersion + 1)
			if config.Num != state.CurrentVersion {
				kv.Ttracef("Upgrading to version %d", config.Num)
				// meet new peers
				for gid, servers := range config.Groups {
					kv.createGroupClerk(gid, servers)
				}
				// configure the leader version
				upgradeArgs := ShardKV_Action_Args_RequestConfigChange{
					Config:     config,
					NewVersion: config.Num,
				}
				_, err := selfClerk.Action_RequestConfigChange(upgradeArgs, true, true, true)
				if err != OK {
					time.Sleep(100 * time.Millisecond)
					continue RETRY
				}
				kv.Ttracef("Upgraded to version %d", config.Num)
			}
		}
	}
}

func (kv *ShardKV) Dtracef(msg string, args ...interface{}) {
	if ShardKVDaemonTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[SKvD][%d-%d-%d-%d][%d-%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), kv.gid, kv.me, m)
	}
}
func (kv *ShardKV) Ttracef(msg string, args ...interface{}) {
	if ShardKVSidecarTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[SKvT][%d-%d-%d-%d][%d-%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), kv.gid, kv.me, m)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	kv := new(ShardKV)
	kv.me = me
	kv.inner = ShardKV_StartServer(servers, me, persister, maxraftstate, gid)

	kv.make_end = make_end
	kv.ctrlerClerk = shardctrler.MakeClerk(ctrlers)
	kv.peerClerk = make(map[int]*ShardKV_Clerk)
	kv.peerClerk[gid] = ShardKV_MakeClerk(servers)
	kv.gid = gid

	kv.rf = kv.inner.rf
	go kv.sidecar()
	return kv
}

///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
// Automatically generated code below.
// I don't want to touch Golang for a second time.
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////

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
		log.Panicf("return type check failed %+v.", ret)
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
		log.Panicf("return type check failed %+v.", ret)
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
		log.Panicf("return type check failed %+v.", ret)
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
		log.Panicf("return type check failed %+v.", ret)
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
		log.Panicf("return type check failed %+v.", ret)
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
		log.Panicf("return type check failed %+v.", ret)
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
		log.Panicf("return type check failed %+v.", ret)
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
	ShardKV_TraceEnabled       = true
	ShardKV_ClientTraceEnabled = true
)
const (
	ShardKVDaemonTraceEnabled  = true
	ShardKVSidecarTraceEnabled = true
	TransferLogEnabled         = true
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
//	kv.tracef("Pending requests for term %d: %+v", term, keys)
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
