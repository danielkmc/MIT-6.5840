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
	"strings"
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

func mapTask(mapf func(string, string) []KeyValue) bool {
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
		err = file.Close()
		if err != nil {
			log.Fatalf("error closing file %v", filename)
		}
		kva := mapf(filename, string(content))

		sort.Sort(ByKey(kva))
		// create intermediary files
		encoders := make([]*json.Encoder, reply.NReduce)
		intermediates := make([]string, reply.NReduce)
		intermediatesFD := []*os.File{}
		parentDir, err := ioutil.TempDir("./", "tmp-intermediates")
		if err != nil {
			log.Fatal("error creating temporary directory for intermediates")
		}

		defer os.RemoveAll(parentDir)

		for i := 0; i < reply.NReduce; i++ {
			oname := fmt.Sprintf("mr-%v-%v.*", reply.TaskNumber, i)
			ofile, err := ioutil.TempFile(parentDir, oname)
			intermediates[i] = ofile.Name()
			if err != nil {
				log.Fatalf("error creating intermediate tempfile %v", oname)
			}
			encoders[i] = json.NewEncoder(ofile)
			intermediatesFD = append(intermediatesFD, ofile)
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
		for _, file := range intermediatesFD {
			err := file.Close()
			if err != nil {
				log.Fatalf("error closing intermediate file %v", file.Name())
			}
		}
		// store intermediates
		_, ok := CallStoreIntermediateFiles(reply.TaskNumber, intermediates)
		if !ok {
			// Can't reach coordinator, exit
			return true
		}

	} else if reply.RemainingTasks != 0 && filename == "" {
		// Remaining tasks are all being processed by other workers
		// Stay on standby incase map tasks are freed
		time.Sleep(time.Duration(50) * time.Millisecond)
		return false
	} else if reply.RemainingTasks == 0 {
		return true
	}
	return false
}

func reduceTask(reducef func(string, []string) string) bool {
	reply, ok := CallGetReduceTasks()
	if !ok {
		// Can't reach Coordinator
		return true
	}
	if reply.RemainingTasks == 0 {
		// Done with all reduce tasks
		return true
	} else if reply.TaskNumber == -1 {
		// there are still reduce tasks remaining but all are assigned
		time.Sleep(time.Duration(50) * time.Millisecond)
		return false
	} else {
		// read intermediary files (should be in sorted order)
		intermediate := []KeyValue{}
		for _, filename := range reply.IntermediateFiles {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open intermediate file %v", filename)
			}

			// get decoder
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("error closing file %v", filename)
			}
		}
		sort.Sort(ByKey(intermediate))
		parentDir, err := ioutil.TempDir("./", "tmp-reduce")
		if err != nil {
			log.Fatalf("error creating reduce temporary directory")
		}

		defer os.RemoveAll(parentDir)

		oname := fmt.Sprintf("mr-out-%v.*", reply.TaskNumber)
		ofile, err := ioutil.TempFile(parentDir, oname)
		if err != nil {
			log.Fatalf("error creating reduce tmpfile %v", oname)
		}

		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		err = ofile.Close()
		if err != nil {
			log.Fatalf("error closing reduce tmpfile %v", oname)
		}

		filename := strings.Split(ofile.Name(), "/")[2]
		i = strings.LastIndex(filename, ".")
		newpath := filename[:i]
		err = os.Rename(ofile.Name(), newpath)
		if err != nil {
			log.Fatalf("error renaming reduce output %v", ofile.Name())
		}
		if !CallCompleteReduceTask(reply.TaskNumber) {
			// Can't reach coordinator, exit
			return true
		}
	}
	return false
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		if mapTask(mapf) {
			break
		}
	}
	for {
		if reduceTask(reducef) {
			break
		}
	}
}

func CallMapTask() MapTaskReply {
	// retrieves map task
	// returns remaining mapping tasks left
	args := MapTaskArgs{}
	reply := MapTaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallStoreIntermediateFiles(task int, intermediates []string) (IntermediateReply, bool) {

	args := IntermediateArgs{}
	args.IntermediaryFiles = intermediates
	args.TaskNumber = task
	reply := IntermediateReply{}

	ok := call("Coordinator.StoreIntermediateFiles", &args, &reply)
	if !ok {
		fmt.Printf("call StoreIntermediateFiles failed!\n")
	}

	return reply, ok
}

func CallGetReduceTasks() (ReduceTaskReply, bool) {
	args := ReduceTaskArgs{}
	reply := ReduceTaskReply{}

	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if !ok {
		fmt.Printf("call GetReduceTask failed!\n")
	}

	return reply, ok
}

func CallCompleteReduceTask(taskNumber int) bool {
	args := ReduceCompletionArgs{}
	args.TaskNumber = taskNumber
	reply := ReduceCompletionReply{}

	ok := call("Coordinator.CompleteReduceTask", &args, &reply)
	if !ok {
		fmt.Printf("call CompleteReduceTask failed!\n")
	}
	return ok
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
