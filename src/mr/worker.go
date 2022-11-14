package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		resp := RequestJob()
		switch resp.TaskType {
		case MapJob:
			log.Println("Assigned a MapJob")
			doMapTask(resp, mapf)
		case ReduceJob:
			log.Println("Assigned a ReduceJob")
			doReduceTask(resp, reducef)
		case WaitJob:
			log.Println("Assigned a WaitJob")
			time.Sleep(1 * time.Second)
		case CompletedJob:
			// the worker process should exit when the job is completely finished.
			return
		default:
			panic(fmt.Sprintf("unexpected job type: %v", resp.TaskType))
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func RequestJob() *AssignJobResponse {
	resp := AssignJobResponse{}
	call("Coordinator.AssignJob", &AssignJobRequest{}, &resp)
	return &resp
}

func ReportJob(taskId int, taskType JobType) *ReportJobResponse {
	req := ReportJobRequest{TaskId: taskId, TaskType: taskType}
	resp := ReportJobResponse{}
	call("Coordinator.ReportJob", &req, &resp)
	return &resp
}

// Do map task
// parses key/value pairs out of input data and passes pair to the user-defined
// Map function.
// The intermediate key/value pairs produces by the Map function are stored in
// local files.
func doMapTask(resp *AssignJobResponse, mapf func(string, string) []KeyValue) {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	filePath := resp.FilePath
	log.Println("open file: ", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

	kva := mapf(filePath, string(content))
	// the map phase should divide the intermediate keys into buckets
	// for nReduce tasks.
	intermediates := make([][]KeyValue, resp.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % resp.NReduce
		intermediates[idx] = append(intermediates[idx], kv)
	}

	var wg sync.WaitGroup
	for idx, intermediate := range intermediates {
		wg.Add(1)
		go func(idx int, intermediate []KeyValue) {
			defer wg.Done()
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				if err := enc.Encode(&kv); err != nil {
					log.Fatal(err)
				}
			}
			intermediateFilePath := generateMapResultFileName(resp.TaskId, idx)
			if err := atomicWriteToFile(intermediateFilePath, &buf); err != nil {
				log.Print("atomicWriteToFile err: ", err)
			}
		}(idx, intermediate)
	}
	wg.Wait()
	ReportJob(resp.TaskId, resp.TaskType)
}

// Read data from local disks of the map workers
// Sort intermediate keys so that all occurrences of the same key are grouped together
// Iterate over the sorted intermediate data and for each unique intermediate key
// encountered, it passes the key and the corresponding set of intermediate values
// to the user's Reduce function. The output of the Reduce function is appended to
// a final output file. for this reduce partition
func doReduceTask(resp *AssignJobResponse, reducef func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < resp.NMap; i++ {
		filePath := generateMapResultFileName(i, resp.TaskId)
		log.Printf("reduce task open file: %v \n", filePath)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				// log.Println(err)
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// if the amount of intermediate data is too large to fit
	// in memory, an external sort is used.
	results := make(map[string][]string)
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}

	var buf bytes.Buffer
	for k, v := range results {
		output := reducef(k, v)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(&buf, "%v %v\n", k, output)
	}
	atomicWriteToFile(generateReduceResulFileName(resp.TaskId), &buf)
	ReportJob(resp.TaskId, resp.TaskType)
}
