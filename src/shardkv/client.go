package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	skvClerks map[int]*ShardKV_Clerk
	kvserial  int64
	kvid      int64
	// You will have to modify this struct.
}

func (ck *Clerk) GetClerkByGID(gid int) *ShardKV_Clerk {
	if ck.skvClerks[gid] == nil {
		var ends []*labrpc.ClientEnd
		for _, end := range ck.config.Groups[gid] {
			ends = append(ends, ck.make_end(end))
		}
	}
	return ck.skvClerks[gid]
}

const (
	ShardKVClerkTraceEnabled = true
)

func (ck *Clerk) tracef(msg string, args ...interface{}) {
	if ShardKVDaemonTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[SKvD][%d-%d-%d-%d]%s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), m)
	}
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.kvserial = 1
	ck.skvClerks = make(map[int]*ShardKV_Clerk)
	ck.kvid = nrand()
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) createGroupClerk(gid int, servers []string) *ShardKV_Clerk {
	var endpoints []*labrpc.ClientEnd
	for _, server := range servers {
		endpoints = append(endpoints, ck.make_end(server))
	}
	return ShardKV_MakeClerk(endpoints)
}
func (ck *Clerk) meetNewPeers() {
	for gid, servers := range ck.config.Groups {
		if ck.skvClerks[gid] == nil {
			ck.tracef("Meeting new group %d", gid)
			ck.skvClerks[gid] = ck.createGroupClerk(gid, servers)
		}

	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := ShardKV_Action_Args_Get{
		Key:          key,
		ClientId:     ck.kvid,
		ClientSerial: ck.kvserial,
	}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		kvclerk := ck.GetClerkByGID(gid)
	ONE_GROUP:
		for {
			if kvclerk != nil {
				reply, err := kvclerk.Action_Get(args, false, false, false)
				if err == OK {
					ck.tracef("Get success with %+v", reply)
					if reply.OutDated {
						ck.kvserial++
						args.ClientSerial = ck.kvserial
					}
					if reply.TryAgainLater {
						time.Sleep(100 * time.Millisecond)
						continue ONE_GROUP
					}
					if reply.WrongGroup {
						// change group.
						break ONE_GROUP
					}

					ck.kvserial++
					return reply.Value
				} else {
					continue
					// outdated. Try again
				}
			}
			break ONE_GROUP
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		ck.meetNewPeers()
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := ShardKV_Action_Args_PutAppend{
		Key:          key,
		Value:        value,
		Op:           op,
		ClientId:     ck.kvid,
		ClientSerial: ck.kvserial,
	}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		kvclerk := ck.GetClerkByGID(gid)
	ONE_GROUP:
		for {
			if kvclerk != nil {
				reply, err := kvclerk.Action_PutAppend(args, false, false, false)
				if err == OK {
					ck.tracef("Put success with %+v", reply)
					if reply.OutDated {
						ck.kvserial++
						return
					}
					if reply.TryAgainLater {
						time.Sleep(100 * time.Millisecond)
						continue ONE_GROUP
					}
					if reply.WrongGroup {
						// change group.
						break ONE_GROUP
					}
					ck.kvserial++
					return
				} else {
					continue
					// outdated. Try again
				}
			}
			break ONE_GROUP
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		ck.meetNewPeers()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
