package shardkv

import (
	"fmt"
	"log"
	"time"

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

const (
	ShardKVDaemonTraceEnabled  = false
	ShardKVSidecarTraceEnabled = false
	TransferLogEnabled         = false
)

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
