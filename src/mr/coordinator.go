package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// We first propose some tools for resource management: retrying queue.
// It is a retrying queue with finish indicator, allowing cascading of Retrying queues.

type RetryingQueue struct {
	lock     *sync.Mutex
	cond     *sync.Cond
	elements *list.List
	remain   int
}

func CreateRetryingQueue(objects []interface{}) RetryingQueue {
	q := RetryingQueue{}
	q.lock = &sync.Mutex{}
	q.cond = sync.NewCond(q.lock)
	q.elements = list.New()
	for _, elem := range objects {
		q.elements.PushBack(elem)
	}
	q.remain = len(objects)
	return q
}
func (c *RetryingQueue) done() bool {
	c.lock.Lock()
	ret := c.remain == 0
	c.lock.Unlock()
	return ret
}
func (c *RetryingQueue) consume() (bool, interface{}) {
	c.lock.Lock()
	for c.remain > 0 && c.elements.Len() == 0 {
		c.cond.Wait()
	}
	if c.remain == 0 {
		c.lock.Unlock()
		c.cond.Broadcast()
		return false, nil
	} else {
		v := c.elements.Front()
		e := v.Value
		c.elements.Remove(v)
		c.lock.Unlock()
		c.cond.Signal()
		return true, e
	}
}
func (c *RetryingQueue) finish() {
	c.lock.Lock()
	c.remain--
	c.lock.Unlock()
	c.cond.Signal()
}
func (c *RetryingQueue) retry(obj interface{}) {
	c.lock.Lock()
	c.elements.PushBack(obj)
	c.lock.Unlock()
	c.cond.Signal()
}

// Coordinator is a coordinator.
//
type Coordinator struct {
	// Your definitions here.
	mapTasks       RetryingQueue
	reduceTasks    RetryingQueue
	id             int64
	runningTasks   map[int64]chan int64
	allMapTasks    int64
	allReduceTasks int64
	done           int32
	lock           *sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

/// This handler may block the goroutine.
/// Returns 0 when there are no new tasks.
/// Assume c is locked.
func (c *Coordinator) nextTask() (bool, interface{}) {
	a, va := c.mapTasks.consume()
	if a {
		return true, va
	} else {
		b, vb := c.reduceTasks.consume()
		if b {
			return true, vb
		} else {
			return false, nil
		}

	}
}

func (c *Coordinator) FetchTask(args *RequestTask, reply *ResponseTask) error {
	flag, task := c.nextTask()

	if flag {
		reply.Ok = true
		c.lock.Lock()
		id := c.id + 1
		c.id++
		reply.TaskId = id
		res := make(chan int64)
		c.runningTasks[id] = res
		c.lock.Unlock()
		switch task := task.(type) {

		case MapTaskDescriptor:
			// map task
			t := MapTask{}
			t.Desc = task
			t.NReduce = c.allReduceTasks
			reply.Task = t
			log.Printf("Map task %d(%s) starting...", task.Id, task.Filename)
			go func() {
				select {
				case <-res:
					log.Printf("Map task %d(%s) finished.", task.Id, task.Filename)
					c.mapTasks.finish()
				case <-time.After(10 * time.Second):
					log.Printf("Map task %d(%s) failed. Retrying...", task.Id, task.Filename)
					c.mapTasks.retry(task)
				}
			}()
		case int64:
			// reduce task
			t := ReduceTask{}
			t.NMap = c.allMapTasks
			t.ReduceId = task
			reply.Task = t
			log.Printf("Reduce task %d starting...", t.ReduceId)
			go func() {
				select {
				case <-res:
					log.Printf("Reduce task %d finished.", t.ReduceId)
					c.reduceTasks.finish()
				case <-time.After(10 * time.Second):
					log.Printf("Reduce task %d failed. Retrying...", t.ReduceId)
					c.reduceTasks.retry(task)
				}
				c.lock.Lock()
				delete(c.runningTasks, id)
				c.lock.Unlock()
			}()
		}
		return nil

	} else {
		reply.Task = nil
		reply.Ok = false
		return nil
	}
}

func (c *Coordinator) FinishTask(args *RequestTaskFinish, reply *ResponseTaskFinish) error {
	c.lock.Lock()
	elem, ok := c.runningTasks[args.TaskId]
	c.lock.Unlock()
	if ok {
		elem <- 0
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	Register()
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done function.
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.mapTasks.done() && c.reduceTasks.done()
	// Your code here.

	//return ret
}

// MakeCoordinator function.
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	arrFiles := make([]interface{}, len(files))
	for i := 0; i < len(files); i++ {
		arrFiles[i] = MapTaskDescriptor{Id: int64(i), Filename: files[i]}
	}
	c.mapTasks = CreateRetryingQueue(arrFiles)
	arrReduce := make([]interface{}, nReduce)
	for i := 0; i < nReduce; i++ {
		arrReduce[i] = int64(i)
	}
	c.reduceTasks = CreateRetryingQueue(arrReduce)
	c.id = 0
	c.runningTasks = make(map[int64]chan int64)
	c.done = 0
	c.lock = &sync.Mutex{}
	c.allMapTasks = int64(len(files))
	c.allReduceTasks = int64(nReduce)

	c.server()
	return &c
}
