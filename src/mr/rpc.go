package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
	"os"
	"strconv"
)

type MapTaskDescriptor struct {
	Id       int64
	Filename string
}

type RequestTask struct {
}

type MapTask struct {
	Desc    MapTaskDescriptor
	NReduce int64
}

type ReduceTask struct {
	ReduceId int64
	NMap     int64
}

type ResponseTask struct {
	Ok     bool
	TaskId int64
	Task   interface{}
}

type RequestTaskFinish struct {
	TaskId int64
}

type ResponseTaskFinish struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func Register() {
	gob.Register(MapTask{})
	gob.Register(MapTaskDescriptor{})
	gob.Register(ReduceTask{})
}
