package mr

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// define each task status
type TaskStatus uint8

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// task definition
// task only updated by coordinator
type Task struct {
	// file path for specifing file
	filePath string
	// unique id for every task
	id int
	// indicate the current state of task
	taskStatus TaskStatus
	// use to check out the worker time out
	startTime time.Time
}

// define each Job type
type JobType uint8

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompletedJob
)

func (j JobType) String() string {
	switch j {
	case MapJob:
		return "MapJob"
	case ReduceJob:
		return "ReduceJob"
	case WaitJob:
		return "WaitJob"
	case CompletedJob:
		return "CompletedJob"
	default:
		panic(fmt.Sprintf("Unexpected job type %d", j))
	}
}

// execution overview of the current MapReduce process
type ExecutionPhase uint8

const (
	MapPhase ExecutionPhase = iota
	ReducePhase
	CompletedPhase
)

func (e ExecutionPhase) String() string {
	switch e {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case CompletedPhase:
		return "CompletedPhase"
	default:
		panic(fmt.Sprintf("Unexpected execution phase: %d", e))
	}
}

// Generate file name for intermediate files
// with map task id and reduce task id
func generateMapResultFileName(mapTaskId, reduceTaskId int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

// the worker should put the output of X'th reduce task in
// the file mr-out-X
func generateReduceResulFileName(reduceTaskId int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskId)
}

// to ensure that nobody observes partially written files in the
// presence of crashes, use a temporary file and atomiccally reanme
// it once it is completed.
func atomicWriteToFile(filePath string, content io.Reader) error {
	// to 2022-11-11T07:26:49
	dirName, fileName := filepath.Split(filePath)
	// the worker should put the intermediate map output file
	// in the current directory, where worker can read them
	// as input to the reduce task.
	if dirName == "" {
		dirName = "."
	}

	// ioutil.TempFile is deprecated use os.CreateTemp instead.
	file, err := os.CreateTemp(dirName, fileName)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.Copy(file, content); err != nil {
		log.Fatal(err)
	}

	if err := os.Rename(file.Name(), filePath); err != nil {
		log.Fatal(err)
	}

	return nil
}
