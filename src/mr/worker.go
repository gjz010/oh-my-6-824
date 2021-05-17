package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func WriteFileAtomically(filename string, content string) error {
	temp, err := ioutil.TempFile("", "-mr-worker-tmpfile")
	if err != nil {
		return err
	}
	_, err = temp.WriteString(content)
	if err != nil {
		temp.Close()
		return err
	}
	path := temp.Name()
	temp.Close()
	err = os.Rename(path, filename)
	if err != nil {
		return err
	}
	return nil
}

var registered bool = false

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
OUTER:
	for true {
		task := ResponseTask{}
		call("Coordinator.FetchTask", &RequestTask{}, &task)
		switch t := task.Task.(type) {
		case MapTask:
			file, err := os.Open(t.Desc.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", t.Desc.Filename)
				file.Close()
				continue OUTER
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", t.Desc.Filename)
				file.Close()
				continue OUTER
			}
			file.Close()
			arr := mapf(t.Desc.Filename, string(content))

			buffers := make([]*bytes.Buffer, t.NReduce)
			encoders := make([]*json.Encoder, t.NReduce)
			for i := 0; i < int(t.NReduce); i++ {
				buffers[i] = bytes.NewBufferString("")
				encoders[i] = json.NewEncoder(buffers[i])
			}
			for _, kv := range arr {
				err := encoders[ihash(kv.Key)%int(t.NReduce)].Encode(&kv)
				if err != nil {
					log.Fatalf("encode %v failed", t.Desc.Filename)
					continue OUTER
				}
			}

			for i := 0; i < int(t.NReduce); i++ {
				name := fmt.Sprintf("mr-%v-%v", t.Desc.Id, i)
				err := WriteFileAtomically(name, buffers[i].String())
				if err != nil {
					log.Fatalf("rename %v failed", name)
					continue OUTER
				}
			}

		case ReduceTask:
			allEntries := make(map[string][]string)
			for i := 0; i < int(t.NMap); i++ {
				name := fmt.Sprintf("mr-%v-%v", int(i), int(t.ReduceId))
				file, err := os.Open(name)
				if err != nil {
					log.Fatalf("open %v failed", name)
					continue OUTER
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					u, v := allEntries[kv.Key]
					if !v {
						u = make([]string, 0)
					}
					u = append(u, kv.Value)
					allEntries[kv.Key] = u
				}
			}
			buffer := bytes.NewBufferString("")
			for k, v := range allEntries {
				val := reducef(k, v)
				fmt.Fprintf(buffer, "%v %v\n", k, val)
			}
			name := fmt.Sprintf("mr-out-%v", int(t.ReduceId))
			err := WriteFileAtomically(name, buffer.String())
			if err != nil {
				log.Fatalf("rename %v failed", name)
				continue OUTER
			}
		}
		res := ResponseTaskFinish{}
		call("Coordinator.FinishTask", &RequestTaskFinish{TaskId: task.TaskId}, &res)
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	if !registered {
		Register()
		registered = true
	}
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
