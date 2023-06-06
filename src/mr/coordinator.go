package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	inputFiles           []string
	MapStartTimes        []time.Time
	MapBool              []bool
	mapMu                sync.Mutex
	remainingMapTasks    int
	IntermediaryFiles    [][]string
	intermediateMu       sync.Mutex
	ReduceStartTimes     []time.Time
	ReduceBool           []bool
	remainingReduceTasks int
	reduceMu             sync.Mutex
	nReduce              int
	nMap                 int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator) GetMapTask(args *MapTaskArgs, reply *MapTaskReply) error {

	c.mapMu.Lock()
	reply.Filename = ""
	for i, starttime := range c.MapStartTimes {
		if starttime.IsZero() {
			reply.Filename = c.inputFiles[i]
			c.MapStartTimes[i] = time.Now()
			reply.TaskNumber = i
			break
		}
	}
	c.mapMu.Unlock()

	c.intermediateMu.Lock()
	// give number of map tasks that aren't complete
	reply.RemainingTasks = c.remainingMapTasks
	c.intermediateMu.Unlock()
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) StoreIntermediateFiles(args *IntermediateArgs, reply *IntermediateReply) error {
	// check if we already have intermediate files for the file name that was processed
	task := args.TaskNumber
	c.intermediateMu.Lock()
	if !c.MapBool[task] {
		// sorted by reduce task, ascending
		for i, filename := range args.IntermediaryFiles {
			// rename files
			name := strings.Split(filename, "/")[2]
			dotSplitIndex := strings.LastIndex(name, ".")
			newpath := name[:dotSplitIndex]
			err := os.Rename(filename, newpath)
			if err != nil {
				log.Fatalf("error renaming temporary intermediate file at: %v", filename)
			}
			c.IntermediaryFiles[i] = append(c.IntermediaryFiles[i], newpath)
		}
		c.MapBool[task] = true
		c.remainingMapTasks -= 1
	}
	c.intermediateMu.Unlock()
	return nil
}

func (c *Coordinator) GetReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	// Get a reduce task
	// Check if intermediate is done for any of the reduce tasks
	// Requirements:
	// 1. All intermediate files are present
	// 2. ReduceStarttime IsZero
	c.reduceMu.Lock()
	if c.remainingMapTasks != 0 {
		// nil indicates that there are no possibletasks since intermediate files are still being stored
		c.reduceMu.Unlock()
		reply.TaskNumber = -1
		return nil
	}
	c.reduceMu.Unlock()
	// There are tasks that can be given out
	c.reduceMu.Lock()
	reply.RemainingTasks = c.remainingReduceTasks
	for i, status := range c.ReduceBool {
		if !status && c.ReduceStartTimes[i].IsZero() {
			c.ReduceStartTimes[i] = time.Now()
			reply.IntermediateFiles = c.IntermediaryFiles[i]
			reply.TaskNumber = i
			// fmt.Printf("Giving task %v\n", i)
			break
		}
	}
	c.reduceMu.Unlock()

	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceCompletionArgs, reply *ReduceCompletionReply) error {
	c.reduceMu.Lock()
	// fmt.Printf("completed task %v!\n", args.TaskNumber)
	c.ReduceBool[args.TaskNumber] = true
	c.remainingReduceTasks -= 1
	c.reduceMu.Unlock()

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// Check if map times need to be reset
	c.mapMu.Lock()
	c.intermediateMu.Lock()
	for i, status := range c.MapBool {
		if !status && !c.MapStartTimes[i].IsZero() {
			morethan10 := time.Since(c.MapStartTimes[i]).Seconds() > 10
			if morethan10 {
				// Allow reissue of this time
				c.MapStartTimes[i] = time.Time{}
			}
		}
	}
	c.mapMu.Unlock()
	c.intermediateMu.Unlock()

	// check if reduce times need to be reset
	c.reduceMu.Lock()
	for i, status := range c.ReduceBool {
		if !status && !c.ReduceStartTimes[i].IsZero() {
			morethan10 := time.Since(c.ReduceStartTimes[i]).Seconds() > 10
			if morethan10 {
				c.ReduceStartTimes[i] = time.Time{}
				// fmt.Printf("Resetting time for reduce job %v\n", i)
			}
		}
	}

	if c.remainingReduceTasks == 0 {
		ret = true
	}
	c.reduceMu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.inputFiles = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.remainingMapTasks = c.nMap
	c.remainingReduceTasks = nReduce

	c.IntermediaryFiles = make([][]string, nReduce)
	c.ReduceBool = make([]bool, nReduce)
	c.MapBool = make([]bool, len(files))
	c.MapStartTimes = make([]time.Time, len(files))
	c.ReduceStartTimes = make([]time.Time, nReduce)

	c.server()
	return &c
}
