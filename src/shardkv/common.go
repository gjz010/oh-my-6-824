package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK = "OK"
	//ErrNoKey       = "ErrNoKey"
	ErrWrongLeader                = "ErrWrongLeader"
	ErrOutdatedOp                 = "ErrOutdatedOp"
	ErrKilled                     = "ErrKilled"
	ErrButItIsYouWhoTimedoutFirst = "ErrButItIsYouWhoTimedoutFirst"
	ErrBadSend                    = "ErrBadSend"
)

type Err string
