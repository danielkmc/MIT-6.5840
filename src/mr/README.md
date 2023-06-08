# MapReduce
Author: Daniel Chen

Note: the original template of this codebase is based on (and owned by) [MIT's 6.5840 Distributed Systems course](https://pdos.csail.mit.edu/6.824/). 

This directory contains the Golang implementation for the Worker and Coordinator (Master) service within MapReduce based on the [original paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) specifying the design.

The application being transformed with MapReduce is a word counter found in ```src/mrapps/wc.go```

There are three implemented files located in ```src/mr```:
* rpc.go
* coordinator.go
* worker.go

where ```coordinator.go``` and ```worker.go``` are called by ```mrcoordinator.go``` and ```mrworker.go```, respectively, located in ```src/main```.

## Components
---
### **rpc.go**
This file contains args and reply structs for the various RPCs implemented in coordinator.go. There are four different pairs of args/reply structs for the four different RPCs implemented: 
* MapTask
  * Args
    * no fields are added here since the worker doesn't need to provide any information to the coordinator when requesting a map task
  * Reply
    * **Filename** - file the map task needs to create key value pairs of the format {string, "1"} for each word
    * **NReduce** - needed to distribute key-value pairs evenly for N reduce jobs. Used to mod hash output for distribution
    * **RemainingTasks** - coordinator tells worker how many tasks are left and if it should wait before performing reduce tasks
    * **TaskNumber** - used to organize intermediate output file names according to the map task
* Intermediate
  * Args
    * **TaskNumber** - used for worker to tell Coordinator which task it was assigned. If the Coordinator already has the output for that map task, it doesn't do anything. Otherwise, it stores the intermediate files and records completion of the map task
    * **IntermediaryFiles** - array of file names of the intermediate files that the map task produced for the Coordinator to store
  * Reply
    * No entries since there is no information to pass to worker
* ReduceTask
  * Args
    * No entries since worker is a resource to the Coordinator to use
  * Reply
    * **TaskNumber** - used for worker to correctly name output of reduce function file
    * **IntermediateFiles** - intermediate files produced by map function for worker to reduce
    * **RemainingTasks** - used to indicate if worker should wait for more tasks or exit
* ReduceCompletion
  * Args
    * **TaskNumber** - used to tell Coordinator that this reduce task number completed
  * Reply
    * No entries since worker will request in separate RPC for new task

---
### **coordinator.go**
This file contains implementation details for the various RPCs needed to perform MapReduce. 

RPCs include: 

* **GetMapTask** - Coordinator provides Worker request details to perform an available map task or tells the Worker to sleep if all the tasks are assigned but may not have been completed.
* **StoreIntermediateFiles** - Coordinator stores intermediate files produced by map task completed by Worker
* **GetReduceTask** - Coordinator provides Worker.intermediate files to perform reduce task or tells Worker to sleep if all tasks are assigned but have not completed.
* **CompleteReduceTask** - Coordinator takes **TaskNumber** provided by Worker to mark the reduce tasks as completed.

The coordinator contains a **Coordinator** struct with various information used to track the state for the MapReduce operation being performed, all initialized in the **MakeCoordinator** function within this file:

```golang
type Coordinator struct {
	inputFiles           []string       // stores input files provided
	mapStartTimes        []time.Time    // stores start times (task time limit 10s)
	mapMu                sync.Mutex     // protects the inputFiles, mapStartTimes
	mapBool              []bool         // stores completion status of task
	remainingMapTasks    int            // stores number of incomplete Map tasks
	intermediaryFiles    [][]string     // [i][j] refers to the jth map reduce output, ith reduce task
	intermediateMu       sync.Mutex     // protects mapBool, remainingMapTasks, intermediaryFiles
	reduceStartTimes     []time.Time    // reduce tasks' start times (max 10s)
	reduceBool           []bool         // reduce tasks' completion status
	remainingReduceTasks int            // number of incomplete reduce tasks
	reduceMu             sync.Mutex     // protects reduceStartTimes, reduceBool, remainingReduceTasks
	nReduce              int            // number of reduce tasks
	nMap                 int            // number of map tasks
}
```

#### **Done()** function
This function performs the critical task of checking if assigned tasks by the Coordinator should be available for reassignment given they have been executing for longer than 10s. The original assignments are still allowed to respond, but if there are free Workers they can also perform these tasks. 

This function is called every second by ```src/main/mrcoordinator.go``` in the main thread. 

---
### **worker.go**

The worker implementation contains the code for reading in files for the map and reduce functions. **Worker()** performs two loops: one to perform the mapping and storage of intermediates, and the other to read in the intermediates, perform the reduce task, and store the results. Both loops: 
1. Exit
    1. if the reply from the Coordinator on request for a map/reduce task responds with 0 remaining tasks left
    2. if the Coordinator is unreachable. 
 2. Continue executing
    1. if there is a task provided
    2. if there are tasks executing and not completed remaining
       1. In this case, the Worker will sleep for 50 milliseconds to prevent repetitive polling of the Coordinator for work (this could be modified to use a Conditional variable in the Coordinator's response)
```golang
func mapTask(mapf func(string, string) []KeyValue) bool {
	reply := CallMapTask()
	filename := reply.Filename
	// If provided a filename, proceed
	if filename != "" {
		//...
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
		//...
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
```
To ensure fault-tolerance, Workers write to temporary files that only are renamed if
1. the Coordinator receives the intermediates files from a mapping task
2. the Worker completes writing reduce output and is considered complete if the Worker is able to communicate to the Coordinator to record completion

These are to follow the specification mentioned in **Section 3.2 - Semantics in the Presence of Failures** of the original MapReduce paper.

For the first case, waiting for the Coordinator to rename the files ensures that files exist and that only one Map task is considered by the Coordinator since the Coordinator will only consider it if it still believes the task has not been completed yet. 

The second case qualifies the condition of using the atomic rename function to ensure there is only one output per reduce task. 