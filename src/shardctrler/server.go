package shardctrler

import (
	"log"
	"sort"

	"6.824/labrpc"
	"6.824/raft"
)

func InitialConfig() []Config {
	nc := Config{}
	nc.Num = 0
	nc.Groups = make(map[int][]string)
	var ret []Config
	ret = append(ret, nc)
	return ret
}
func (config *Config) cloneConfig() Config {
	nc := Config{}
	nc.Num = config.Num
	nc.Shards = config.Shards
	nc.Groups = make(map[int][]string)
	for k, v := range config.Groups {
		nc.Groups[k] = v
	}
	return nc
}
func (state *ShardController_State) cloneLastConfig() Config {
	config := state.ConfigVersions[len(state.ConfigVersions)-1]
	return config.cloneConfig()
}
func (state *ShardController_State) pushConfig(config Config) {
	state.ConfigVersions = append(state.ConfigVersions, config)
}
func pop(alist *[]int) int {
	f := len(*alist)
	rv := (*alist)[f-1]
	*alist = append((*alist)[:f-1])
	return rv
}
func (config *Config) makeShardMap() [NShards + 1][]int {
	var shardCountMap [NShards + 1][]int
	m := make(map[int]int)
	appearedNodes := make(map[int]int)
	for gid := range config.Groups {
		appearedNodes[gid] = 1
	}
	for _, gid := range config.Shards {
		if gid > 0 {
			m[gid]++

		}
	}
	for gid, shard := range m {
		shardCountMap[shard] = append(shardCountMap[shard], gid)
		appearedNodes[gid] = 0
	}
	for gid, v := range appearedNodes {
		if v == 1 {
			shardCountMap[0] = append(shardCountMap[0], gid)
		}
	}
	for _, arr := range shardCountMap {
		sort.Ints(arr)
	}
	return shardCountMap
}
func (state *ShardController_State) TransferAction_Join_Impl(args ShardController_Action_Args_Join) ShardController_Action_Reply_Join {
	config := state.cloneLastConfig()
	config.Num++
	originallyEmpty := len(config.Groups) == 0
	for gid, server := range args.Servers {
		config.Groups[gid] = server
	}
	if originallyEmpty {
		// round-robin hash
		var newServers []int
		for gid := range args.Servers {
			newServers = append(newServers, gid)
		}
		sort.Ints(newServers)
		for shard := range config.Shards {
			config.Shards[shard] = newServers[(shard % len(newServers))]
		}
		state.pushConfig(config)
		return ShardController_Action_Reply_Join{Magic: 2}
	}
	// We perform the rebalance in this way:
	// We find the cluster with most elements and least elements
	// If the difference is at most one, give up.
	// Otherwise, move one.
	shardCountMap := config.makeShardMap()
	//log.Printf("%v %v", config, shardCountMap)
	takeouts := make(map[int]int)
	takeins := make(map[int]int)
	func() {
		for {
			shardMin := 0
			shardMax := NShards
			for (shardMax > (shardMin + 1)) && len(shardCountMap[shardMax]) == 0 {
				shardMax--
			}
			if shardMax <= shardMin+1 {
				return
			}
			for shardMin < shardMax-1 && len(shardCountMap[shardMin]) == 0 {
				shardMin++
			}
			if shardMin >= shardMax-1 {
				return
			}
			// take out first element.
			// still deterministic
			gid_out := pop(&shardCountMap[shardMax])
			gid_in := pop(&shardCountMap[shardMin])
			if takeins[gid_out] == 0 {
				takeouts[gid_out]++
			} else {
				log.Panicf("bad")
				takeins[gid_in]--
			}
			if takeouts[gid_in] == 0 {
				takeins[gid_in]++
			} else {
				log.Panicf("bad")
				takeouts[gid_in]--
			}

			shardCountMap[shardMin+1] = append(shardCountMap[shardMin+1], gid_in)
			shardCountMap[shardMax-1] = append(shardCountMap[shardMax-1], gid_out)
		}
	}()
	for shard, gid := range config.Shards {
		if takeouts[gid] > 0 {
			takeouts[gid]--
			config.Shards[shard] = 0
		}
	}
	takein_keys := make([]int, 0)
	for k, v := range takeins {
		if v > 0 {
			takein_keys = append(takein_keys, k)
		}
	}
	sort.Ints(takein_keys)
	i := 0
	for shard, gid := range config.Shards {
		if gid == 0 {
			for takeins[takein_keys[i]] == 0 {
				i++
			}
			takeins[takein_keys[i]]--
			config.Shards[shard] = takein_keys[i]
		}
	}
	state.pushConfig(config)
	return ShardController_Action_Reply_Join{Magic: 1}
}
func (state *ShardController_State) TransferAction_Leave_Impl(args ShardController_Action_Args_Leave) ShardController_Action_Reply_Leave {
	config := state.cloneLastConfig()
	config.Num++

	removedGIDs := make(map[int]int)
	for _, gid := range args.GIDs {
		removedGIDs[gid] = 1
		delete(config.Groups, gid)

	}
	if len(config.Groups) == 0 {
		// Removed all shards. Special handling
		for shard := range config.Shards {
			config.Shards[shard] = 0
		}
		state.pushConfig(config)
		return ShardController_Action_Reply_Leave{Magic: 1}
	}
	removedShards := make([]int, 0)
	for shard, gid := range config.Shards {
		if removedGIDs[gid] == 1 {
			config.Shards[shard] = 0
			removedShards = append(removedShards, shard)
		}
	}
	sort.Ints(removedShards)
	shardCountMap := config.makeShardMap()
	i := 0
	for _, shard := range removedShards {
		for len(shardCountMap[i]) == 0 {
			i++
		}
		nextGid := pop(&shardCountMap[i])
		config.Shards[shard] = nextGid
		// still deterministic
		shardCountMap[i+1] = append(shardCountMap[i+1], nextGid)
	}
	state.pushConfig(config)
	return ShardController_Action_Reply_Leave{Magic: 1}
}
func (state *ShardController_State) TransferAction_Move_Impl(args ShardController_Action_Args_Move) ShardController_Action_Reply_Move {
	config := state.cloneLastConfig()
	config.Num++
	config.Shards[args.Shard] = args.GID
	state.pushConfig(config)
	return ShardController_Action_Reply_Move{Magic: 1}
}
func (state *ShardController_State) TransferAction_Query_Impl(args ShardController_Action_Args_Query) ShardController_Action_Reply_Query {
	/* TODO: Implement Query */
	id := args.Num
	if id == -1 || id >= len(state.ConfigVersions) {
		id = len(state.ConfigVersions) - 1
	}
	return ShardController_Action_Reply_Query{
		Config: state.ConfigVersions[id].cloneConfig(),
	}
}

type ShardCtrler struct {
	inner *ShardController_ReplicatedStateMachine
	rf    *raft.Raft
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.inner.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.inner.rf
}

func (sc *ShardCtrler) ShardController_Action(args *ShardController_ActionArgs, reply *ShardController_ActionReply) {
	sc.inner.ShardController_Action(args, reply)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	inner := ShardController_StartServer(servers, me, persister, 0)

	sc := &ShardCtrler{inner: inner}
	sc.rf = inner.rf
	return sc
}
