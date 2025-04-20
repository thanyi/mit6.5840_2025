package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type GlobalStatus int

type TaskStatus int // 描述任务状态

const (
	UnAssigned TaskStatus = iota
	Assigned
	Failed
	Completed
)

type TaskInfo struct {
	TaskStatus TaskStatus
	FileName   string
	TimeStamp  time.Time // 表示时间戳
}
type Coordinator struct {
	nReduce              int // 表明reduce worker的数目
	nMap                 int
	mutx                 sync.Mutex
	MapTaskInfos         []TaskInfo
	ReduceTaskInfos      []TaskInfo
	IsAllMapCompleted    bool
	IsAllReduceCompleted bool
	NMapAssigned         int
	NReduceAssigned      int
}

// 作为Coordinator的主要函数
func (c *Coordinator) RequestTask(args *MsgSend, reply *MsgReply) error {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	/* 首先分析维护的Map任务列表是否还存在可分Map任务 */
	if !c.IsAllMapCompleted {
		NMapAssigned := 0 // 初始化，不然每次申请计数就变大
		// 若是还有任务，则进行划分
		for idx, taskInfo := range c.MapTaskInfos {
			if taskInfo.TaskStatus == UnAssigned || taskInfo.TaskStatus == Failed ||
				(taskInfo.TaskStatus == Assigned && time.Since(taskInfo.TimeStamp) >= 10*time.Second) {
				// Reply值的赋予
				reply.NReduce = c.nReduce
				reply.TaskId = idx // 将idx作为返回值中的TaskId
				reply.TaskType = MapTask
				reply.FileName = taskInfo.FileName
				reply.NMap = c.nMap

				c.MapTaskInfos[idx].TaskStatus = Assigned
				c.MapTaskInfos[idx].TimeStamp = time.Now()
				return nil
			} else if taskInfo.TaskStatus == Completed {
				NMapAssigned++
			}
		}

		if NMapAssigned == len(c.MapTaskInfos) {
			c.IsAllMapCompleted = true
		} else {
			reply.TaskType = WaitTask
			return nil
		}

	}

	if !c.IsAllReduceCompleted {
		NReduceAssigned := 0
		// 若是还有任务，则进行划分
		for idx, taskInfo := range c.ReduceTaskInfos {
			if taskInfo.TaskStatus == UnAssigned || taskInfo.TaskStatus == Failed ||
				(taskInfo.TaskStatus == Assigned && time.Since(taskInfo.TimeStamp) >= 10*time.Second) {
				// Reply值的赋予
				reply.NReduce = c.nReduce
				reply.TaskId = idx
				reply.TaskType = ReduceTask
				reply.FileName = taskInfo.FileName
				reply.NMap = c.nMap
				// Coordinator中的ReduceTaskInfos结构体数组的赋值
				c.ReduceTaskInfos[idx].TaskStatus = Assigned
				c.ReduceTaskInfos[idx].TimeStamp = time.Now()
				return nil
			} else if taskInfo.TaskStatus == Completed {
				NReduceAssigned++
			}
		}

		if NReduceAssigned == len(c.ReduceTaskInfos) {
			c.IsAllReduceCompleted = true
		} else {
			reply.TaskType = WaitTask
			return nil
		}

	}
	reply.TaskType = DoneTask
	return nil
}

func (c *Coordinator) WorkDoneTask(args *MsgSend, reply *MsgReply) error {
	c.mutx.Lock()
	defer c.mutx.Unlock()

	taskId := args.TaskId
	// 根据worker的返回更新结构体数组的状态
	switch args.WorkState {
	case MapCompleted:
		c.MapTaskInfos[taskId].TaskStatus = Completed
	case MapFailed:
		c.MapTaskInfos[taskId].TaskStatus = Failed
	case ReduceCompleted:
		c.ReduceTaskInfos[taskId].TaskStatus = Completed
	case ReduceFailed:
		c.ReduceTaskInfos[taskId].TaskStatus = Failed
	}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.IsAllMapCompleted && c.IsAllReduceCompleted {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:              nReduce,
		nMap:                 len(files),
		MapTaskInfos:         make([]TaskInfo, len(files)),
		ReduceTaskInfos:      make([]TaskInfo, nReduce),
		IsAllMapCompleted:    false,
		IsAllReduceCompleted: false,
	}
	c.initTask(files) // 初始化任务列表
	c.server()
	return &c
}

func (c *Coordinator) initTask(files []string) {
	// 初始化MapTaskInfos
	for idx, file := range files {
		c.MapTaskInfos[idx] = TaskInfo{
			TaskStatus: UnAssigned,
			FileName:   file,
			TimeStamp:  time.Now(),
		}
	}

	// 初始化ReduceTaskInfos
	for idx, _ := range c.ReduceTaskInfos {
		c.ReduceTaskInfos[idx] = TaskInfo{
			TaskStatus: UnAssigned,
			TimeStamp:  time.Now(),
		}
	}

}
