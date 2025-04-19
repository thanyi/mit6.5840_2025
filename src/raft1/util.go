package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

var (
	debugStart     time.Time
	debugVerbosity int
)

func init() {
	debugStart = time.Now()
	debugVerbosity = getVerbosity()
	// 将日志的输出格式修改为不包含日期和时间戳。
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func DebugPrintf(topic logTopic, serverId int, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		t := time.Since(debugStart).Microseconds()
		t /= 100
		prefix := fmt.Sprintf("%06d %v S%d ", t, string(topic), serverId)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
