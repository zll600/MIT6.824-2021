package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const MaxTaskTimeOut = time.Second * 10

// indicate state of current worker
// when deal with AssignJobRequest
// notify Start goroutine
type assignJobMsg struct {
	resp *AssignJobResponse
	ok   chan struct{}
}

// indicate state of current worker
// when deal with ReportJobRequest
// notify Start goroutine
type reportJobMsg struct {
	req *ReportJobRequest
	ok  chan struct{}
}

type Coordinator struct {
	// Your definitions here.
	// count of files user input
	files []string
	// Map tasks reduce tasks
	tasks []Task
	// count of Map tasks == files
	nMap int
	// count of Reduce tasks user set
	nReduce int
	// execution phase
	// map --> reduce --> completed
	phase ExecutionPhase

	assignJobCh chan assignJobMsg
	reportJobCh chan reportJobMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignJob(req *AssignJobRequest, resp *AssignJobResponse) error {
	assignJob := assignJobMsg{resp, make(chan struct{})}
	c.assignJobCh <- assignJob
	<-assignJob.ok
	return nil
}

func (c *Coordinator) ReportJob(req *ReportJobRequest, resp *ReportJobResponse) error {
	reportJob := reportJobMsg{req, make(chan struct{})}
	c.reportJobCh <- reportJob
	<-reportJob.ok
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

// Start Map execution phase
func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files))
	// generate c.files tasks
	for idx, file := range c.files {
		c.tasks[idx] = Task{
			filePath:   file,
			id:         idx,
			taskStatus: Idle,
		}
	}
}

// Start Reduce execution phase
func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	// generate nReduce tasks
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:         i,
			taskStatus: Idle,
		}
	}
}

func (c *Coordinator) initCompletedPhase() {
	c.phase = CompletedPhase
	c.doneCh <- struct{}{}
}

// choose a task for a worker
func (c *Coordinator) selectTask(resp *AssignJobResponse) bool {
	hasTaskIdle, allPhaseCompleted := false, true
	for idx, task := range c.tasks {
		switch task.taskStatus {
		case Idle:
			// has task can give this worker
			hasTaskIdle = true
			// the entire execution hasn't finished
			allPhaseCompleted = false
			c.tasks[idx].taskStatus, c.tasks[idx].startTime = InProgress, time.Now()

			resp.FilePath, resp.TaskId, resp.NMap, resp.NReduce = task.filePath, idx, c.nMap, c.nReduce
			if c.phase == MapPhase {
				resp.TaskType = MapJob
			} else {
				resp.TaskType = ReduceJob
			}
		case InProgress:
			// the entire execution hasn't finished
			allPhaseCompleted = false
			// Wait for ten seconds and then give up and re-issue
			// the task to a differcent worker
			// the coordinator assume the worker has died(even if may be not have)
			if time.Now().Sub(task.startTime) > MaxTaskTimeOut {
				hasTaskIdle = true
				// this task should remain in-progress status
				// so don't change tis taskStatus
				// c.tasks[idx].taskStatus = Idle
				c.tasks[idx].startTime = time.Now()

				resp.FilePath, resp.TaskId, resp.NMap, resp.NReduce = task.filePath, idx, c.nMap, c.nReduce
				if c.phase == MapPhase {
					resp.TaskType = MapJob
				} else {
					resp.TaskType = ReduceJob
				}
			}
		case Completed:
			// we cann't change task status to Idle
			// we must wait all map or reduce task finishe
			// ten we can change task status to idle
		}

		// if still have task can give worker
		if hasTaskIdle {
			break
		}
	}
	// reduce task can't start util the last map has finished
	// so worker should sleep for a while
	if !hasTaskIdle {
		resp.TaskType = WaitJob
	}

	return allPhaseCompleted
}

func (c *Coordinator) Start() {
	c.initMapPhase()
	for {
		select {
		case assignJob := <-c.assignJobCh:
			if c.selectTask(assignJob.resp) {
				switch c.phase {
				case MapPhase:
					log.Printf("Coordinator: %v finished, %v Start", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(assignJob.resp)
				case ReducePhase:
					log.Printf("Coordinator: %v finished, %v Start", ReducePhase, CompletedPhase)
					c.initCompletedPhase()
					assignJob.resp.TaskType = CompletedJob
				case CompletedPhase:
					panic("")
				}
			}
			log.Printf("Coordinator: assign a task %v to a worker \n", assignJob.resp)
			assignJob.ok <- struct{}{}
		case reportJob := <-c.reportJobCh:
			log.Printf("Coordinator: worker has executed the task %v \n", c.tasks[reportJob.req.TaskId])
			c.tasks[reportJob.req.TaskId].taskStatus = Completed
			reportJob.ok <- struct{}{}
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// return true when the MapReduce job complete finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case <-c.doneCh:
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		assignJobCh: make(chan assignJobMsg),
		reportJobCh: make(chan reportJobMsg),
		doneCh:      make(chan struct{}),
	}

	// Your code here.

	c.server()
	go c.Start()
	return &c
}
