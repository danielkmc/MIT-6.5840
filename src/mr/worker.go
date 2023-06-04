package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapTask(reply MapTaskReply) bool {
	// Performs file read, map task,
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		startTime := time.Now()
		reply := CallMapTask()
		filename := reply.Filename
		// If provided a filename, proceed
		if filename != "" {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			sort.Sort(ByKey(kva))
			// create intermediary files
			encoders := make([]*json.Encoder, reply.NReduce)
			intermediates := make([]string, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				oname := fmt.Sprintf("mr-out-%v-%v", reply.TaskNumber, i)
				intermediates[i] = oname
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatalf("error creating intermediate file %v", oname)
				}
				encoders[i] = json.NewEncoder(ofile)
			}
			// write to the intermediate file now
			// read the key, values in
			for _, kv := range kva {
				reduceTask := ihash(kv.Key) % reply.NReduce
				err := encoders[reduceTask].Encode(&kv)
				if err != nil {
					log.Fatalf("error encoding KeyValue for intermediate %v", reduceTask)
				}
			}
			// store intermediates
			CallStoreIntermediateFiles(reply.TaskNumber, intermediates)
		}

		if reply.RemainingTasks != 0 {
			endTime := time.Now()
			if duration := int(startTime.Sub(endTime).Seconds()); duration < 10 {
				sleepTime := time.Duration(10-duration) * time.Second
				time.Sleep(sleepTime)
			} else {
				continue
			}
		} else {
			break
		}
	}

	// Get reduce task if there are no more map tasks

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallMapTask() MapTaskReply {
	// retrieves map task
	// returns remaining mapping tasks left
	args := MapTaskArgs{}

	reply := MapTaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallStoreIntermediateFiles(task int, intermediates []string) IntermediateReply {

	args := IntermediateArgs{}
	args.IntermediaryFiles = intermediates
	args.TaskNumber = task

	reply := IntermediateReply{}

	ok := call("Coordinator.StoreIntermediateFiles", &args, &reply)
	if ok {
		fmt.Printf("call success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
