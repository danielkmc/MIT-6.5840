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
	mapStartTimes        []time.Time
	mapBool              []bool
	mapMu                sync.Mutex
	remainingMapTasks    int
	intermediaryFiles    [][]string
	intermediateMu       sync.Mutex
	reduceStartTimes     []time.Time
	reduceBool           []bool
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
	for i, starttime := range c.mapStartTimes {
		if starttime.IsZero() {
			reply.Filename = c.inputFiles[i]
			c.mapStartTimes[i] = time.Now()
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
	if !c.mapBool[task] {
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
			c.intermediaryFiles[i] = append(c.intermediaryFiles[i], newpath)
		}
		c.mapBool[task] = true
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
	reply.TaskNumber = -1
	c.reduceMu.Lock()
	reply.RemainingTasks = c.remainingReduceTasks
	if c.remainingReduceTasks > 0 {
		// There are tasks that can be given out
		for i, status := range c.reduceBool {
			if !status && c.reduceStartTimes[i].IsZero() {
				c.reduceStartTimes[i] = time.Now()
				reply.IntermediateFiles = append(reply.IntermediateFiles, c.intermediaryFiles[i]...)
				reply.TaskNumber = i
				break
			}
		}
	}
	c.reduceMu.Unlock()

	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceCompletionArgs, reply *ReduceCompletionReply) error {
	// Record completion. Worker handles atomic renaming of file since it can overwrite as many times as possible
	c.reduceMu.Lock()
	c.reduceBool[args.TaskNumber] = true
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
	for i, status := range c.mapBool {
		if !status && !c.mapStartTimes[i].IsZero() {
			morethan10 := time.Since(c.mapStartTimes[i]).Seconds() > 10
			if morethan10 {
				// Allow reissue of this time
				c.mapStartTimes[i] = time.Time{}
			}
		}
	}
	c.mapMu.Unlock()
	c.intermediateMu.Unlock()

	// check if reduce times need to be reset
	c.reduceMu.Lock()
	for i, status := range c.reduceBool {
		if !status && !c.reduceStartTimes[i].IsZero() {
			morethan10 := time.Since(c.reduceStartTimes[i]).Seconds() > 10
			if morethan10 {
				c.reduceStartTimes[i] = time.Time{}
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

	c.intermediaryFiles = make([][]string, nReduce)
	c.reduceBool = make([]bool, nReduce)
	c.mapBool = make([]bool, len(files))
	c.mapStartTimes = make([]time.Time, len(files))
	c.reduceStartTimes = make([]time.Time, nReduce)

	c.server()
	return &c
}
