package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	inner *ShardController_Clerk
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	inner := ShardController_MakeClerk(servers)
	ck := &Clerk{inner: inner}
	return ck
}

func (ck *Clerk) Query(num int) Config {
	for {
		args := ShardController_Action_Args_Query{Num: num}
		ret, outdated := ck.inner.Action_Query(args)
		if !outdated {
			return ret.Config
		}
		// otherwise, try to get again.
		time.Sleep(100 * time.Millisecond)
	}

}

func (ck *Clerk) Join(servers map[int][]string) {
	args := ShardController_Action_Args_Join{Servers: servers}
	ck.inner.Action_Join(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := ShardController_Action_Args_Leave{GIDs: gids}
	ck.inner.Action_Leave(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := ShardController_Action_Args_Move{Shard: shard, GID: gid}
	ck.inner.Action_Move(args)
}
