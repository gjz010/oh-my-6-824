package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers                                     []*labrpc.ClientEnd
	lastLeader                                  int
	clientId                                    int64
	clientSerial                                int64
	lockSoThatClientWillNotSendMultipleRequests sync.Mutex
	// You will have to modify this struct.
}

func (ck *Clerk) getNextSerial() int64 {
	serial := ck.clientSerial
	ck.clientSerial++
	return serial
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientSerial = 0
	ck.clientId = nrand()
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) tracef(msg string, args ...interface{}) {
	if ClientTraceEnabled {
		m := fmt.Sprintf(msg, args...)
		now := time.Now()
		fmt.Printf("[KVCk][%d-%d-%d-%d][%d] %s\n", now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), ck.clientId, m)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.lockSoThatClientWillNotSendMultipleRequests.Lock()
	defer ck.lockSoThatClientWillNotSendMultipleRequests.Unlock()
	leader := ck.lastLeader
	args := GetArgs{Key: key, ClientId: ck.clientId, ClientSerial: ck.getNextSerial()}
	reply := GetReply{}
	ck.tracef("Sending request %+v", args)
GET_LOOP:
	for {
		ck.tracef("Trying server %d", leader)
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
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
			break GET_LOOP
		case ErrKilled:
			ck.tracef("Killed")
			reply = GetReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrWrongLeader:
			ck.tracef("Wrong leader")
			reply = GetReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrOutdatedOp:
			// well let's try again.
			// This time it will succeed.
			reply = GetReply{}
			args.ClientSerial = ck.getNextSerial()
			continue GET_LOOP
		case ErrButItIsYouWhoTimedoutFirst:
			log.Panicln("This should not happen.")
		}
	}

	ck.lastLeader = leader
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.lockSoThatClientWillNotSendMultipleRequests.Lock()
	defer ck.lockSoThatClientWillNotSendMultipleRequests.Unlock()
	leader := ck.lastLeader
	args := PutAppendArgs{Key: key, ClientId: ck.clientId, ClientSerial: ck.getNextSerial(), Value: value, Op: op}
	reply := PutAppendReply{}
	ck.tracef("Sending request %+v", args)
PUT_LOOP:
	for {
		ck.tracef("Trying server %d", leader)
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.tracef("Sending request %+v failed.", args)
			reply = PutAppendReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
			continue // try again
		}
		switch reply.Err {
		case OK:
			break PUT_LOOP
		case ErrKilled:
			ck.tracef("Killed.")
			reply = PutAppendReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrWrongLeader:
			ck.tracef("Wrong leader.")
			reply = PutAppendReply{}
			leader++
			if leader >= len(ck.servers) {
				leader = 0
			}
		case ErrOutdatedOp:
			// Okay for put/append ops.
			break PUT_LOOP
		case ErrButItIsYouWhoTimedoutFirst:
			log.Panicln("This should not happen.")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
