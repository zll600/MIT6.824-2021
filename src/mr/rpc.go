package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
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

// get job request
type AssignJobRequest struct{}

// get job response
type AssignJobResponse struct {
	// to find input file
	FilePath string
	// indicate current state of worker
	TaskType JobType
	// for indicating number in mapf output file
	TaskId int
	// the total number of worker executing mapf
	NMap int
	// the total number of workers executing reducef
	NReduce int
}

func (a *AssignJobResponse) String() string {
	switch a.TaskType {
	case MapJob, ReduceJob:
		return fmt.Sprintf("{TaskType: %v, FilePath: %v, TaskId: %v, NMap %v, NReduce: %v}", a.TaskType, a.FilePath, a.TaskId, a.NMap, a.NReduce)
	case WaitJob, CompletedJob:
		return fmt.Sprintf("{TaskType: %v, TaskId: %v}", a.TaskType, a.TaskId)
	}
	return fmt.Sprintf("unexpected TaskType: %d", a.TaskType)
}

type ReportJobRequest struct {
	// id of the current task
	TaskId int
	// job type of current task
	TaskType JobType
}

func (r *ReportJobRequest) String() string {
	return fmt.Sprintf("{TaskType: %v, TaskId: %v}", r.TaskType, r.TaskId)
}

type ReportJobResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
