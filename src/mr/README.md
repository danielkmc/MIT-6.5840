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
### worker.go