package mr

import (
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type State int

const (
	IDLE State = iota
	IN_PROGRESS
	COMPLETED
	WAIT
)

type Type int

const (
	MAP Type = iota
	REDUCE
)

type Coordinator struct {
	unDispatchMapTasks []Task
	mapTasks           []Task
	IntermediateFile   map[string][]string
	reduceTasks        []Task

	uDLock sync.Mutex
}

type Task struct {
	ID        int
	State     State
	Location  []string
	Size      int64
	Type      Type
	ReduceNum int
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DispatchWork(args *DispatchWorkArgs, reply *DispatchWorkReply) error {

	c.uDLock.Lock()
	defer c.uDLock.Unlock()
	if len(c.unDispatchMapTasks) != 0 {

		task := c.unDispatchMapTasks[0]
		task.State = IN_PROGRESS
		task.StartTime = time.Now()
		reply.Task = task
		reply.Bool = true
		c.unDispatchMapTasks = c.unDispatchMapTasks[1:]
		c.mapTasks = append(c.mapTasks, task)
		//fmt.Println("dispatch map Work,now unDispatch map work num is" + strconv.Itoa(len(c.unDispatchMapTasks)))
		return nil
	}
	if len(c.IntermediateFile) != 0 && len(c.unDispatchMapTasks) == 0 && len(c.mapTasks) == 0 {
		for k, v := range c.IntermediateFile {
			reduceTask := Task{
				ID:        generateID(),
				State:     IN_PROGRESS,
				Location:  v,
				Size:      0,
				Type:      REDUCE,
				ReduceNum: 10,
				StartTime: time.Now(),
			}
			reply.Task = reduceTask
			reply.Bool = true
			c.reduceTasks = append(c.reduceTasks, reduceTask)
			delete(c.IntermediateFile, k)
			return nil
		}

	}

	//fmt.Println(strconv.Itoa(len(c.unDispatchMapTasks)) + "-" + strconv.Itoa(len(c.mapTasks)) + "-" +
	//strconv.Itoa(len(c.IntermediateFile)) + "-" + strconv.Itoa(len(c.reduceTasks)))
	reply.Bool = false
	return nil
}

func generateID() int {
	return rand.Intn(9999999)
}

func (c *Coordinator) WorkDone(args *WorkDoneArgs, reply *WorkDoneReply) error {
	c.uDLock.Lock()
	defer c.uDLock.Unlock()
	task := args.Task
	// 将完成的任务删除
	c.removeTask(task.ID, task.Type)

	// 如果是Map任务保存中间文件地址
	if task.Type == MAP {

		for _, file := range args.Locations {
			files := c.IntermediateFile[strings.Split(file, "-")[2]]
			if files == nil {
				files = []string{}
			}

			map1 := map[string]string{}
			map1[file] = file
			for _, file := range files {
				map1[file] = file
			}

			files = []string{}
			for k, _ := range map1 {
				files = append(files, k)
			}

			c.IntermediateFile[strings.Split(file, "-")[2]] = files
			reply.Bool = true
		}
	}
	return nil
}

// removeTask 删除已经完成的任务
func (c *Coordinator) removeTask(taskId int, taskType Type) {

	if taskType == MAP {
		for i, task := range c.mapTasks {
			if task.ID == taskId {
				c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
				return
			}
		}
	} else if taskType == REDUCE {
		for i, task := range c.reduceTasks {
			if task.ID == taskId {
				c.reduceTasks = append(c.reduceTasks[:i], c.reduceTasks[i+1:]...)
				return
			}
		}
	}

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
	c.uDLock.Lock()
	defer c.uDLock.Unlock()
	return len(c.unDispatchMapTasks) == 0 && len(c.mapTasks) == 0 &&
		len(c.IntermediateFile) == 0 && len(c.reduceTasks) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.IntermediateFile = make(map[string][]string)
	i := 0
	for _, file := range files {
		task := Task{
			ID:        i,
			State:     IDLE,
			Location:  []string{file},
			Size:      222,
			Type:      MAP,
			ReduceNum: nReduce,
		}
		c.unDispatchMapTasks = append(c.unDispatchMapTasks, task)
		i++
	}

	go func() {
		for {
			time.Sleep(time.Second * 5)
			c.uDLock.Lock()
			mapTasks := c.mapTasks
			reduceTasks := c.reduceTasks
			if mapTasks != nil {
				for i, mapTask := range mapTasks {
					if mapTask.State == IN_PROGRESS {
						now := time.Now()
						if now.Sub(mapTask.StartTime).Seconds() > 5 {
							newTask := Task{
								ID:        mapTask.ID,
								State:     IDLE,
								Location:  mapTask.Location,
								Size:      mapTask.Size,
								Type:      mapTask.Type,
								ReduceNum: mapTask.ReduceNum,
								StartTime: mapTask.StartTime,
							}
							c.unDispatchMapTasks = append(c.unDispatchMapTasks, newTask)
							c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
							break
						}
					}
				}
			}

			if reduceTasks != nil {
				for i, reduceTask := range reduceTasks {
					if reduceTask.State == IN_PROGRESS {
						now := time.Now()
						if now.Sub(reduceTask.StartTime).Seconds() > 5 {
							c.IntermediateFile[strings.Split(reduceTask.Location[0], "-")[2]] = reduceTask.Location
							c.reduceTasks = append(c.reduceTasks[:i], c.reduceTasks[i+1:]...)
							break
						}
					}
				}
			}
			c.uDLock.Unlock()
		}
	}()

	c.server()
	return &c
}
