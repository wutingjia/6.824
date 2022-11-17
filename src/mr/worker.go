package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		args := &DispatchWorkArgs{X: 1}
		reply := &DispatchWorkReply{}
		ok := call("Coordinator.DispatchWork", args, reply)
		if !ok {
			fmt.Printf("call failed!\n")
			return
		}

		if !reply.Bool {
			time.Sleep(3 * time.Second)
			continue
		}

		task := reply.Task
		if task.Type == MAP {
			doMapWork(mapf, task)
		} else {
			doReduceWork(reducef, task)
		}
	}

}

func doReduceWork(reducef func(string, []string) string, task Task) {

	fileNames := task.Location
	var intermediate []KeyValue
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {

			word := strings.Split(line, " ")
			if len(word) < 2 {
				continue
			}
			kv := KeyValue{
				Key:   word[0],
				Value: word[1],
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	outputFile, _ := os.Create("mr-out-" + strings.Split(fileNames[0], "-")[2])

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		str := fmt.Sprintf("%v %v\n", intermediate[i].Key, output)
		outputFile.WriteString(str)
		i = j
	}
	outputFile.Close()

	args := &WorkDoneArgs{Task: task}
	reply := &WorkDoneReply{}

	ok := call("Coordinator.WorkDone", args, reply)

	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func doMapWork(mapf func(string, string) []KeyValue, task Task) {

	filename := task.Location

	file, err := os.Open(filename[0])
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	locations := []string{}
	kva := mapf(filename[0], string(content))

	// key hash值   value（key 单词 value 1）
	kv := map[int][]KeyValue{}
	for _, e := range kva {
		hash := ihash(e.Key) % task.ReduceNum

		kvv := kv[hash]
		if kvv == nil {
			kvv = []KeyValue{}
		}
		kvv = append(kvv, e)
		kv[hash] = kvv
	}

	for k, v := range kv {

		filename := "mr-" + strconv.Itoa(task.ID) + "-" + strconv.Itoa(k)
		if !exit(locations, filename) {
			locations = append(locations, filename)
		}

		var ofile *os.File
		_, err := os.Open(filename)
		if err != nil && os.IsNotExist(err) {
			ofile, _ = os.Create(filename)
		} else {
			ofile, _ = os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0666)
		}

		for _, vv := range v {
			str := fmt.Sprintf("%v %v\n", vv.Key, vv.Value)
			ofile.WriteString(str)
		}
		ofile.Close()
	}

	args := &WorkDoneArgs{Task: task, Locations: locations}
	reply := &WorkDoneReply{}

	ok := call("Coordinator.WorkDone", args, reply)

	if !ok {
		fmt.Printf("call failed!\n")
	}
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

func exit(array []string, str string) bool {
	for _, string1 := range array {
		if string1 == str {
			return true
		}
	}
	return false
}
