package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
			c.IntermediaryFiles[i] = append(c.IntermediaryFiles[i], filename)
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
	var possibletasks []int
	c.intermediateMu.Lock()
	for nTask, intermediates := range c.IntermediaryFiles {
		if len(intermediates) == c.nMap {
			possibletasks = append(possibletasks, nTask)
		}
	}
	c.intermediateMu.Unlock()

	if len(possibletasks) == 0 {
		// nil indicates that there are no possibletasks since intermediate files are still being stored
		return nil
	}
	// Similar to GetMapTask
	// This function will only give out tasks if the start time is 0
	// The start times are reset to 0 by the done() function if it doesn't find a ReduceBool value set to true after 10 seconds
	// The final output files are rewritten each time a reduce task completes to ensure idempotent behavior
	c.reduceMu.Lock()
	for i := range possibletasks {
		if c.ReduceStartTimes[i].IsZero() {
			c.ReduceStartTimes[i] = time.Now()
			reply.IntermediateFiles = c.IntermediaryFiles[i]
			reply.TaskNumber = i
			c.reduceMu.Unlock()
			return nil
		}
	}
	reply.RemainingTasks = c.remainingReduceTasks
	c.reduceMu.Unlock()

	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceCompletionArgs, reply *ReduceCompletionReply) error {
	c.reduceMu.Lock()
	if !c.ReduceBool[args.TaskNumber] {
		c.ReduceBool[args.TaskNumber] = true
		c.remainingReduceTasks -= 1
	}
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
	maptasksremaining := 0
	for inputKey, status := range c.MapBool {
		if !status {
			morethan10 := time.Now().Sub(c.MapStartTimes[inputKey]).Seconds() > 10
			if morethan10 {
				// Allow reissue of this time
				c.MapStartTimes[inputKey] = time.Time{}
				log.Printf("[COORDINATOR] Reset start time for map task: %v. Difference: %v\n", inputKey, morethan10)
			}
			maptasksremaining += 1
		}
	}
	c.mapMu.Unlock()
	c.intermediateMu.Unlock()

	// check if reduce times need to be reset
	c.reduceMu.Lock()
	reducetasksremaining := 0
	for n, status := range c.ReduceBool {
		if !status {
			morethan10 := time.Now().Sub(c.ReduceStartTimes[n]).Seconds() > 10
			if morethan10 {
				// Allow reissue of this time
				c.ReduceStartTimes[n] = time.Time{}
				log.Printf("[COORDINATOR] Reset start time for reduce task: %v. Difference: %v\n", n, morethan10)
			}
			reducetasksremaining += 1
		}
	}
	c.reduceMu.Unlock()

	log.Printf("[COORDINATOR] Map tasks remaining: %v | Reduce tasks remaining: %v\n", maptasksremaining, reducetasksremaining)
	if reducetasksremaining == 0 {
		ret = true
	}

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

	// Initialize nReduce number of intermediary file lists
	for i := 1; i <= nReduce; i++ {
		c.IntermediaryFiles[i] = make([]string, 0)
	}

	c.server()
	return &c
}
