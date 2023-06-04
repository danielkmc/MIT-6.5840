package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// Used to get map task
type MapTaskArgs struct{}

type MapTaskReply struct {
	Filename       string
	NReduce        int
	RemainingTasks int
	TaskNumber     int
}

// Used to return intermediate files and indicated map task completion
type IntermediateArgs struct {
	TaskNumber        int
	IntermediaryFiles []string
}

type IntermediateReply struct{}

// Used to get reduce task
type ReduceTaskArgs struct{}

type ReduceTaskReply struct {
	TaskNumber        int
	IntermediateFiles []string
	RemainingTasks    int
}

// Used to indicate reduce task completion
type ReduceCompletionArgs struct {
	TaskNumber int
}

type ReduceCompletionReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
