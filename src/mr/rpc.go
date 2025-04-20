package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

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

type TaskType int
type WorkStatus int // worker的工作状态
type GlobalState int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	DoneTask
)

const (
	MapCompleted WorkStatus = iota
	MapFailed
	ReduceCompleted
	ReduceFailed
)

type MsgSend struct {
	TaskId    int        // 表明这个worker的编号
	WorkState WorkStatus // 表明worker的状态
}

type MsgReply struct {
	TaskId   int
	TaskType TaskType // 分配的任务类型
	FileName string
	NReduce  int
	NMap     int
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
