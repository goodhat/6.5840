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
type GetWorkArgs struct {
}
type GetWorkReply struct {
	WorkType int    // 1: Map, 2: Reduce, 0: Done, 3: hang
	Filename string // File that map worker should deal with.
	NReduce  int
	Idx      int
}

type MapDoneArgs struct {
	Idx int
}

type MapDoneReply struct {
	Msg string // Not used
}

type ReduceDoneArgs struct {
	Idx int
}

type ReduceDoneReply struct {
	Msg string // Not used
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
