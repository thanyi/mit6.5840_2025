package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"
	"math"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Term    int         //创建该Log时的任期
	Command interface{} //需要执行的命令
}

const (
	TMIN_HeartBeatInterval    time.Duration = 100 * time.Millisecond
	TMIN_ElectionTimeInterval time.Duration = 300 * time.Millisecond
	TM_RandomWaitInterval     time.Duration = 500 * time.Millisecond
	MAX_RETRY_TIMES           int           = 3
)

type State int

const (
	RaftFollower State = iota
	RaftCandidate
	RaftLeader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             State //   节点状态，分为Follower、Leader、Candidate
	nextElectionTime  time.Time
	nextHeartbeatTime time.Time

	// 以下字段论文原文
	currentTerm int        // 当前任期
	votedFor    int        // 投票给的候选者ID
	log         []LogEntry // 日志
	commitIndex int        // 已经提交的最高日志条目索引
	lastApplied int        // 已经应用到状态机的最高日志条目索引
	nextIndex   []int      // 对每个服务器，要发送的下一跳日志条目的索引
	matchIndex  []int      // 对每个服务器，已知被复制的最高日志条目的索引

	// snapshot
	lastIncludedIndex int                   // 快照中最新的Index
	lastIncludedTerm  int                   // 快照中最新的Index中的Term
	SnapShot          []byte                // 保存的snapshot
	applyCh           chan raftapi.ApplyMsg // 注意chan关键字，声明此时
}

// 帮助更新下一次心跳时间点
func (rf *Raft) ResetHeartbeat() {
	now := time.Now()
	rf.nextHeartbeatTime = now.Add(TMIN_HeartBeatInterval) // 心跳时间更新100毫秒
}

// 帮助更新下一次选举时间点
func (rf *Raft) ResetElectionTime() {
	now := time.Now()
	extra := time.Duration(float64(rand.Int63()%int64(TMIN_ElectionTimeInterval)) * 0.7)
	rf.nextElectionTime = now.Add(TMIN_ElectionTimeInterval).Add(extra) // 选举时间更新为300毫秒 * (1 + rand(0.7))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A). Done
	term = rf.currentTerm
	isleader = rf.state == RaftLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		DebugPrintf(dError, rf.me, "Server%d can't persist currTerm%d.", rf.me, rf.currentTerm)
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		DebugPrintf(dError, rf.me, "Server%d can't persist votedFor%d.", rf.me, rf.votedFor)
		return
	}
	// 保存log
	err = e.Encode(rf.log)
	if err != nil {
		DebugPrintf(dError, rf.me, "Server%d can't persist log.", rf.me)
		return
	}

	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		DebugPrintf(dError, rf.me, "Server%d can't persist lastIncludedIndex.", rf.me)
		return
	}

	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		DebugPrintf(dError, rf.me, "Server%d can't persist lastIncludedTerm.", rf.me)
		return
	}

	raftState := w.Bytes()
	// 根据snapshot是否存在来进行Save
	if rf.SnapShot != nil {
		rf.persister.Save(raftState, rf.SnapShot)
	} else {
		rf.persister.Save(raftState, nil)
	}

	DebugPrintf(dPersist, rf.me, "Success for persist.")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm       int
		votedFor          int
		log               []LogEntry
		lastIncludedIndex int
		lastIncludedTerm  int
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DebugPrintf(dError, rf.me, "Server%d can't readPersist.", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	DebugPrintf(dPersist, rf.me, "readPersist Success!")
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// snapshot由上层服务器来进行调用
	// snapshot中已经存储了所有需要保存的快照，在这个函数中只需要把它保存进变量中即可
	// 1. 切除log中被快照的部分 2.保存snapshot

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPrintf(dSnap, rf.me, "Server%d start save snapshot and cut log.", rf.me)

	if index > rf.getLastLogIndex() {
		DebugPrintf(dError, rf.me, "index > len(rf.log) in SnapShot.")
	}

	if index < rf.lastIncludedIndex {
		DebugPrintf(dError, rf.me, "index < rf.lastIncludedIndex in SnapShot.")
	}
	// 在log被修改之前拿到lastIncludedTerm
	rf.lastIncludedTerm = rf.getLogFromIndex(index).Term

	DebugPrintf(dSnap, rf.me, "old Log : %v", rf.log)
	rf.log = rf.getLogSlice(index+1, rf.getLastLogIndex()+1) // 左闭右开
	DebugPrintf(dSnap, rf.me, "new Log : %v", rf.log)

	rf.lastIncludedIndex = index // 先修改log之后再改lastIncludedIndex

	DebugPrintf(dSnap, rf.me, "LastIncludedTerm:%d LastIncludedIndex:%d", rf.lastIncludedTerm, rf.lastIncludedIndex)

	rf.SnapShot = snapshot

	DebugPrintf(dSnap, rf.me, "Server%d end saving snapshot and cut log.", rf.me)
	rf.persist()
}

// InstallSnapshot的RPC
type InstallSnapshotArgs struct {
	// Your data here (3A, 3B).
	Term              int    // 任期号
	LeaderId          int    // 领导者ID
	LastIncludedIndex int    // raft的snap最后一个index
	LastIncludedTerm  int    // LastIncludedIndex对应的index
	Data              []byte // 快照块的原始字节，从 offset 开始
	//Offset            int
	//Done              bool
}

// InstallSnapshot的RPC
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugPrintf(dSnap, rf.me, "Follower%d start to receive snapshot.", rf.me)
	// --------- 任期周期检查 -----------
	// 1.Follower任期与Leader任期比较, 大于直接返回Term
	if rf.currentTerm > args.Term {
		DebugPrintf(dTerm, rf.me, "reject installsnapshot, Term of Leader%d smaller than that of Follower.", args.LeaderId)
		reply.Term = rf.currentTerm
		//rf.mu.Unlock()
		return
	}
	// 2.Follower任期与Leader任期比较, 小于则进行Term调整
	if rf.currentTerm < args.Term {
		DebugPrintf(dTerm, rf.me, "Term of Leader%d bigger than that of Follower.", args.LeaderId)

		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.persist() // 进行持久化

		if rf.state != RaftFollower {
			rf.state = RaftFollower
			DebugPrintf(dInfo, rf.me, "降级为Follower")
		}
		reply.Term = rf.currentTerm
	}
	rf.ResetElectionTime()

	// 判断是否Follower已经提交过index的内容
	if args.LastIncludedIndex <= rf.lastApplied {
		reply.Term = rf.currentTerm
		DebugPrintf(dInfo, rf.me, "args.LastIncludedIndex:%d < rf.lastApplied:%d, have committed the snapshot.", args.LastIncludedIndex, rf.lastApplied)

		return
	}

	if args.LastIncludedIndex > rf.getLastLogIndex() || rf.lastIncludedTerm < args.LastIncludedTerm {
		// 如果index大于log的最后一个index
		// 或者lastIncludeTerm比本地的lastIncludeTerm大
		// 直接清空log
		rf.log = []LogEntry{}
		DebugPrintf(dInfo, rf.me, "snapshot: clear the all the logs.")
	} else {
		// 清空snapshot之前的log
		DebugPrintf(dInfo, rf.me, "snapshot: clear the logs before the args.LastIncludedIndex. ")
		rf.log = rf.getLogSlice(rf.lastIncludedIndex+1, rf.getLastLogIndex()+1)
	}

	reply.Term = rf.currentTerm
	// 状态更新
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.SnapShot = args.Data

	rf.commitIndex = min(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = min(rf.lastApplied, args.LastIncludedIndex)

	// 保存，并且提交上层server
	rf.persist()
	applyMsg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	go rf.commitSnap(applyMsg)

	DebugPrintf(dSnap, rf.me, "Follower%d process snapshot done.", rf.me)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选者的任期号
	CandidateId  int // 候选者ID
	LastLogIndex int // 候选者最新日志的索引
	LastLogTerm  int // 候选者最新日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期
	VoteGranted bool // 是否投票
}

// example RequestVote RPC handler.
// RequestVote由Leader发出，由Follower接收并进行投票
// 所以这里的逻辑是以Follower为视角
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPrintf(dVote, rf.me, "Follower%d receives requests of Candidate%d.", rf.me, args.CandidateId)
	// --------- 任期周期检查 -----------
	// 拒绝情况讨论
	// 1.Follower任期与Candidate任期比较
	if rf.currentTerm > args.Term {
		DebugPrintf(dVote, rf.me, "reject Vote, Term of Candidate%d smaller than that of Follower.", args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 假如候选者任期大于跟随者，跟随者任期更新
	if rf.currentTerm < args.Term {
		// 如果此时状态不是Follower，则需降为Follower
		if rf.state != RaftFollower {
			rf.state = RaftFollower
		}
		DebugPrintf(dTerm, rf.me, "Follower%d update term to：%d", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist() // 持久化存储
	}
	// 2.在相同任期的时候，若是votefor不为-1，表示已经投过票；
	// 不为args.CandidateId，表示不是由于网络原因为同一个候选人投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DebugPrintf(dVote, rf.me, "reject Vote，Follower%d have voted.", rf.me)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//  --------- 选举限制检查 -----------
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	if lastLogTerm >= args.LastLogTerm {
		// 如果Follower的lastLogTerm大于Candidate
		if args.LastLogTerm < lastLogTerm {
			DebugPrintf(dVote, rf.me, "election restriction, Candidate%d's LastLogTerm too small.", args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		// 如果Follower日志长度大于Candidate
		if lastLogIndex > args.LastLogIndex {
			DebugPrintf(dVote, rf.me, "election restriction, Candidate's LastLogTerm equals follower's but LastLogIndex too small.")
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}
	// --------- 开始投票 -----------
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	rf.persist() // 持久化存储
	DebugPrintf(dVote, rf.me, "Follower%d vote for %d", rf.me, rf.votedFor)
	rf.ResetElectionTime()

}

// 日志信息和心跳信息的请求体
// 只有Leader可以发送
type AppendEntriesArgs struct {
	Term         int        // 候选者的任期号
	LeaderId     int        // 首领的ID
	PrevLogIndex int        // 当前日志的前一个日志
	PrevLogTerm  int        // 当前Term的前一个Term
	Entries      []LogEntry // log日志构成的数组
	LeaderCommit int        // Leader的commitID
}

// 日志信息和心跳信息的请求体
// 由Follower们发送
type AppendEntriesReply struct {
	Term    int  // 当前任期
	Success bool // 如果Follower们存有对应的prevLogIndex和prevLogTerm

	//Extend
	XTerm  int
	XIndex int
	XLen   int
}

// 在Lab3A中，只针对心跳信息进行发送
// 逻辑也是在Follower端执行
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugPrintf(dInfo, rf.me, "Follower%d receive AppendEntries. args.prevLogIndex: %d, args.PrevLogTerm : %d , args.entries: %v.", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	// 领导者任期小于当前任期, 直接拒绝
	if args.Term < rf.currentTerm {
		DebugPrintf(dError, rf.me, "Term of Leader%d smaller than Follower%d, reject AppendEntries.", args.LeaderId, rf.me)
		reply.Success = false
		reply.Term = rf.currentTerm

		// Extend
		reply.XTerm = -1  // 不存在
		reply.XIndex = -1 // 不存在
		reply.XLen = rf.getLastLogIndex()
		return
	}
	// 领导者任期大于当前任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		// 如果状态不是follower，降为follower
		if rf.state != RaftFollower {
			DebugPrintf(dError, rf.me, "Receive higher Term%d, down to Follower", rf.currentTerm)
			rf.state = RaftFollower
		}
		rf.votedFor = -1 // 修正投票
		rf.persist()     //
	}
	// 等于当前任期，Candidate遇到心跳信息，降为follower
	if rf.state == RaftCandidate {
		DebugPrintf(dError, rf.me, "Candidate%d receive AppendEntries, down to Follower", rf.me)
		rf.state = RaftFollower
		rf.persist() // 立即持久化
	}
	// 开始进行日志修改
	prevLogIndex := args.PrevLogIndex
	if args.PrevLogIndex < 0 {
		DebugPrintf(dError, rf.me,
			"Something wrong so the previous log index is smaller than 0.")
	}
	// 判断参数中的prevLogIndex是否小于快照最后一项
	// 若是小于，则直接返回， 重点关注这里，不能是小于等于
	if prevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.XTerm = -1  // 不存在
		reply.XIndex = -1 // 不存在
		reply.XLen = rf.getLastLogIndex()
		DebugPrintf(dError, rf.me, "Follower%d find args.PrevLogIndex in snapshot, Reject. LastIncludeIndex = %d", rf.me, rf.lastIncludedIndex)
		return
	}

	// 检查Leader的PrevLogIndex是否直接大于Follower的最后一个日志
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.XTerm = -1  // 不存在
		reply.XIndex = -1 // 不存在
		reply.XLen = rf.getLastLogIndex()
		DebugPrintf(dInfo, rf.me, "PrevLogIndex > rf.getLastLogIndex(): Receive PrevLogIndex:%d, rf.LastLogIndex=%d, log = %v", args.PrevLogIndex, rf.getLastLogIndex(), rf.log)
		return
	}

	// 如果日志在PrevLogIndex的位置不包含Term与PrevLogTerm匹配的Entry，则回复false
	if args.PrevLogTerm != rf.getLogFromIndex(args.PrevLogIndex).Term {
		DebugPrintf(dError, rf.me,
			"Follower%d's PrevLogIndex is %d, Term is %d. But args.PrevLogTerm is %d. Log : %v",
			rf.me,
			args.PrevLogIndex,
			rf.getLogFromIndex(prevLogIndex).Term,
			args.PrevLogTerm, rf.log)

		reply.Success = false
		reply.Term = rf.currentTerm

		reply.XTerm = rf.getLogFromIndex(args.PrevLogIndex).Term
		reply.XLen = rf.getLastLogIndex()
		reply.XIndex, _ = rf.TermRange(reply.XTerm)
		return
	}

	// 如果prev匹配成功
	// 如果当前应更新
	//if rf.getLastLogIndex() > prevLogIndex && len(args.Entries) > 0 {
	//	rf.log = rf.log[:prevLogIndex+1] // 将后面的内容都清空
	//}

	for i, entry := range args.Entries {
		// 第一个判断条件: 判断最后一个log的index是否小于当前要写的log index，符合表示需要追加
		// 第二个判断条件: 判断args.Entries中的第i日志与follower中的对应节点是否存在冲突，存在冲突则进行修复
		if args.PrevLogIndex+1+i > rf.getLastLogIndex() || rf.getLogFromIndex(args.PrevLogIndex+i+1).Term != entry.Term {
			// 注意这里left修改为lastIncludedIndex
			// 注意这里进行冲突检查！！，如果没有这个冲突检测，就会遗漏
			rf.log = append(rf.getLogSlice(rf.lastIncludedIndex+1, args.PrevLogIndex+i+1), args.Entries[i:]...)
			DebugPrintf(dLog, rf.me, "Follower%d update the log : %v, entries : %v", rf.me, rf.log, args.Entries)
			rf.persist() // 更新log
			break
		}
	}

	//  如果leaderCommit > commitIndex，则commitIndex设置为min(leaderCommit,最新Entry的索引)
	if args.LeaderCommit > rf.commitIndex {
		DebugPrintf(dInfo, rf.me, "Receive Leader CommitIdx: %d", args.LeaderCommit)
		// 不能超过Leader的Commit。如果Peer的Log比较滞后，args.PrevLogIndex+len(args.Entries)能快速更新commitIdx
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		DebugPrintf(dCommit, rf.me, "Update CommitIdx to %d, now LastApplied is %d, lastIncludeIndex is %v, log is %v", rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.log)
	}

	// 修改自身commitIndex为Leader发送过来的CommitIndex
	//rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())

	reply.Success = true
	reply.Term = rf.currentTerm
	//reply.FollowerLastApplied = rf.lastApplied
	rf.ResetElectionTime()
	DebugPrintf(dLog, rf.me, "Follower%d AppendEntries Done!", rf.me)
}

func (rf *Raft) StartElection() {
	// 开始进行选举，注意每一个选举需要进行协程操作
	rf.currentTerm += 1
	rf.state = RaftCandidate
	rf.votedFor = rf.me
	rf.persist() // 持久化存储
	DebugPrintf(dVote, rf.me,
		"Candidate%d start election, term：%d", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	go rf.broadcastElection(args)

	rf.ResetElectionTime()
}

// 从Candidate端进行广播请求，请求投票
func (rf *Raft) broadcastElection(args RequestVoteArgs) {
	voteCnt := 1
	//var one sync.Once  不能使用这个one变量进行单独的操作！！！！会触发很奇怪的Bug！！！
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 每针对一个i，生成一个go协程进行投票
		go func(i int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			DebugPrintf(dVote, rf.me, "Candidate%d's request to Follower %d. LastLogIndex=%d, LastLogTerm=%d", rf.me, i, rf.getLastLogIndex(), rf.getLastLogTerm())

			if !rf.sendRequestVote(i, &args, &reply) {
				// 投票过程失败，不做处理
				DebugPrintf(dError, rf.me, "Candidate's request to Follower %d error!", i)
				return
			}

			if reply.Term > rf.currentTerm {
				// 如果返回的Term大于自身，状态变为Follower
				DebugPrintf(dError, rf.me, "Candidate%d's reply.Term from Follower %d is bigger", rf.me, i)
				rf.state = RaftFollower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist() // 持久化存储
				// 状态变为Follower时需要更新时间
				rf.ResetElectionTime()
				return
			}

			if reply.VoteGranted {
				rf.mu.Lock()
				if rf.state == RaftCandidate {
					// 成功的情况，需要加锁，保证对voteCnt操作不会冲突
					voteCnt += 1
					if voteCnt > len(rf.peers)>>1 {
						// todo 查看有无优化，函数封装
						rf.state = RaftLeader
						DebugPrintf(dLeader, rf.me, "server%d up to Leader", rf.me)
						DebugPrintf(dTerm, rf.me, "the Term：%d", rf.currentTerm)

						// 初始化，初始化NextIndex数组和matchIndex数组
						for i, _ := range rf.peers {
							if i != rf.me {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							} else {
								// Leader 自己的 matchIndex 是最新的
								rf.matchIndex[i] = rf.getLastLogIndex()
								// Leader 自己的 nextIndex 是最后一个索引 + 1
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
							}
						}
						// 开始广播发送AppendEntries
						go rf.broadcastHeartbeat()
						rf.ResetHeartbeat()

					}
				}
				rf.mu.Unlock()
			}
		}(i, args)

		// 如果状态变为Follower，表示投票失败，直接退出循环
		if rf.state == RaftFollower {
			DebugPrintf(dError, rf.me, "server%d down to Follower，end election.", rf.me)
			break
		}

		// 如果状态变为Leader，表示已经成功，直接退出循环
		if rf.state == RaftLeader {
			DebugPrintf(dError, rf.me, "server%d down to Follower，end election.", rf.me)
			break
		}
	}
}

// 进行广播心跳信息，对每一个server发送AppendEntries
func (rf *Raft) broadcastHeartbeat() {
	DebugPrintf(dLeader, rf.me, "Leader%d start to send AppendEntries, log: %v", rf.me, rf.log)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.mu.Lock()
			rf.matchIndex[i] = rf.getLastLogIndex()
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.mu.Unlock()
			continue
		}

		// 判断是否是发送快照
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			DebugPrintf(dSnap, rf.me, "Leader%d start to send snapshot to Follower %d, log: %v", rf.me, i, rf.log)
			go rf.sendServerSnapshot(i)
			continue
		}

		// 判断当前是否需要日志发送
		var (
			prevLogIndex int
			prevLogTerm  int
			entries      []LogEntry
			leaderCommit int
		)

		if rf.getLastLogIndex() >= rf.nextIndex[i] {
			entries = rf.getLogSlice(rf.nextIndex[i], rf.getLastLogIndex()+1)
		}

		if rf.getPrevLogIndex(i) >= 0 {
			prevLogIndex = rf.getPrevLogIndex(i)
			prevLogTerm = rf.getPrevLogTerm(i)
		}
		leaderCommit = rf.commitIndex

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		go rf.heartbeat(i, args)
		//go rf.heartbeat(i)
		if rf.state != RaftLeader {
			break
		}
	}
	//rf.updateCommitIndex()
	// 更新心跳时间，如果被降为Follower，则更新选举时间
	rf.ResetHeartbeat()
	if rf.state != RaftLeader {
		rf.ResetElectionTime()
	}

}

// Leader对单个Follower进行心跳信息发送的逻辑
func (rf *Raft) heartbeat(i int, args AppendEntriesArgs) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	reply := AppendEntriesReply{}
	if rf.state != RaftLeader {
		DebugPrintf(dWarn, rf.me, "Not Leader，Reject to do anything.")
		return
	}

	DebugPrintf(dLeader, rf.me, "Send appendEntries to Follower %d, Use PrevLogIndex %d, PrevLogTerm %d, Entries: %v.", i, args.PrevLogIndex, args.PrevLogTerm, args.Entries)

	if !rf.sendAppendEntries(i, &args, &reply) {
		// 信息发送失败
		DebugPrintf(dError, rf.me,
			"For S%d, sendAppendEntries send failed!", i)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		// 接收的任期大于自身任期，降级为Follower
		rf.state = RaftFollower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		DebugPrintf(dError, rf.me, "Leader%d receives higher term in AppendEntries reply, down to Follower, Term update to %d.", rf.me, rf.currentTerm)
		rf.persist() // 持久化存储
		rf.ResetElectionTime()
		return
	}

	if reply.Success {
		DebugPrintf(dLeader, rf.me, "S%d AppendEntry success.", i)
		//DebugPrintf(dLeader, rf.me, "Leader log: %v", rf.log)
		// 更新对应的nextIndex和matchIndex
		//rf.nextIndex[i] = rf.getLastLogIndex() + 1
		//rf.matchIndex[i] = rf.getLastLogIndex()

		//更新peer的nextIdx和matchIdx
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		////计算当前commitIdx，保证幂等性
		rf.nextIndex[i] = max(newNext, rf.nextIndex[i])
		rf.matchIndex[i] = max(newMatch, rf.matchIndex[i])

		rf.updateCommitIndex()

	} else {
		// 重试机制，当心跳信息返回fail时
		if rf.state != RaftLeader {
			DebugPrintf(dError, rf.me, "S%d is not Leader any more.", rf.me)
			return
		}

		DebugPrintf(dError, rf.me, "S%d AppendEntry failed. Leader's log: %v.", i, rf.log)
		if reply.XTerm == -1 {
			// 如果没有XTerm，证明Follower的log长度小于prevLogIndex
			rf.nextIndex[i] = reply.XLen + 1
			DebugPrintf(dLeader, rf.me, "S%d XTerm == -1,nextIdx -> reply.XLen + 1 = %d", i, reply.XLen+1)
		} else {
			// 如果有XTerm
			DebugPrintf(dLeader, rf.me, "S%d reply.XTerm is %d. reply.XIndex is %d ", i, reply.XTerm, reply.XIndex)
			_, maxIdx := rf.TermRange(reply.XTerm)
			if maxIdx == -1 {
				// 如果没有maxIdx，证明Leader没有Follower中的Term，则从这个Term最开始，XIndex的位置重新开始
				rf.nextIndex[i] = reply.XIndex
				DebugPrintf(dLeader, rf.me, "Leader don't have XTerm %d ,S%d nextIdx -> %d", reply.XTerm, i, reply.XIndex)
			} else {
				// 如果有maxIdx，证明Leader有Follower中的Term，则直接以这个maxIdx开始
				rf.nextIndex[i] = maxIdx + 1
				DebugPrintf(dLeader, rf.me, "Leader has XTerm%d ,S%d nextIdx -> %d", reply.XTerm, i, maxIdx)
			}
		}

		// 检查计算出的 nextIndex 是否有效且在 Leader 日志范围内
		if rf.nextIndex[i] < 0 { // 防御性检查
			rf.nextIndex[i] = 0 // 或 1，取决于你的 dummy entry 策略
			DebugPrintf(dError, rf.me, "S%d calculated nextIndex %d < 0. Resetting to %d.", i, rf.nextIndex[i], rf.nextIndex[i])
		}
		if rf.nextIndex[i] > rf.getLastLogIndex()+1 { // 修正条件
			// 如果计算出的 nextIndex 超出了 Leader 自己的日志范围，将其限制为 Leader 日志的末尾
			// Leader 只能发送自己拥有的日志条目
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			DebugPrintf(dError, rf.me, "S%d calculated nextIndex %d > Leader log length %d. Resetting to %d.", i, rf.nextIndex[i], rf.getLastLogIndex()+1, rf.getLastLogIndex()+1)

		}

		// 判断是否需要发送快照
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			DebugPrintf(dSnap, rf.me, "nextIndex < LastIncludedIndex, send snapshot to follower %d. ", i)
			go rf.sendServerSnapshot(i)
			return
		}

		nextIdx := rf.nextIndex[i]
		prevLogIndex := nextIdx - 1
		prevLogTerm := rf.getLogFromIndex(prevLogIndex).Term

		entries := rf.getLogSlice(nextIdx, rf.getLastLogIndex()+1)
		DebugPrintf(dLeader, rf.me, "Down S%d nextIdx, nextIdx:%d", i, rf.nextIndex[i])
		newArg := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		DebugPrintf(dLeader, rf.me, "Leader%d restart appendentries to Follower%d", rf.me, i)
		go rf.heartbeat(i, newArg)
		return
	}

}

//// Leader对单个Follower进行心跳信息发送的逻辑
//func (rf *Raft) heartbeat(i int) {
//
//	if rf.state != RaftLeader {
//		DebugPrintf(dWarn, rf.me, "Not Leader，Reject to do anything.")
//		return
//	}
//
//	for rf.state == RaftLeader {
//		rf.mu.Lock()
//		// 判断当前是否需要日志发送
//		var (
//			prevLogIndex int
//			prevLogTerm  int
//			entries      []LogEntry
//			leaderCommit int
//		)
//		if rf.getLastLogIndex() >= rf.nextIndex[i] {
//			entries = rf.getLogSlice(rf.nextIndex[i], rf.getLastLogIndex()+1)
//		}
//
//		if rf.getPrevLogIndex(i) >= 0 {
//			prevLogIndex = rf.getPrevLogIndex(i)
//			prevLogTerm = rf.getPrevLogTerm(i)
//		}
//		leaderCommit = rf.commitIndex
//
//		args := AppendEntriesArgs{
//			Term:         rf.currentTerm,
//			LeaderId:     rf.me,
//			PrevLogIndex: prevLogIndex,
//			PrevLogTerm:  prevLogTerm,
//			Entries:      entries,
//			LeaderCommit: leaderCommit,
//		}
//		rf.mu.Unlock()
//		reply := AppendEntriesReply{}
//		DebugPrintf(dLeader, rf.me, "Send appendEntries to Follower %d, Use PrevLogIndex %d, PrevLogTerm %d, Entries: %v.", i, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
//
//		if !rf.sendAppendEntries(i, &args, &reply) {
//			// 信息发送失败
//			DebugPrintf(dError, rf.me,
//				"For S%d, sendAppendEntries send failed!", i)
//			return
//		}
//
//		rf.mu.Lock()
//		if reply.Term > rf.currentTerm {
//			// 接收的任期大于自身任期，降级为Follower
//			rf.state = RaftFollower
//			rf.votedFor = -1
//			rf.currentTerm = reply.Term
//			DebugPrintf(dError, rf.me, "Leader%d receives higher term in AppendEntries reply, down to Follower, Term update to %d.", rf.me, rf.currentTerm)
//			rf.persist() // 持久化存储
//			rf.ResetElectionTime()
//
//			rf.mu.Unlock()
//			return
//		}
//
//		if reply.Success {
//			DebugPrintf(dLeader, rf.me, "S%d AppendEntry success.", i)
//
//			//更新peer的nextIdx和matchIdx
//			newNext := args.PrevLogIndex + len(args.Entries) + 1
//			newMatch := args.PrevLogIndex + len(args.Entries)
//			////计算当前commitIdx，保证幂等性
//			rf.nextIndex[i] = max(newNext, rf.nextIndex[i])
//			rf.matchIndex[i] = max(newMatch, rf.matchIndex[i])
//
//			rf.updateCommitIndex()
//
//		} else {
//			// 重试机制，当心跳信息返回fail时
//			DebugPrintf(dError, rf.me, "S%d AppendEntry failed. Leader's log: %v.", i, rf.log)
//			if reply.XTerm == -1 {
//				rf.nextIndex[i] = reply.XLen + 1
//				DebugPrintf(dLeader, rf.me, "S%d XTerm == -1,nextIdx -> reply.XLen + 1 = %d", i, reply.XLen+1)
//			} else {
//				DebugPrintf(dLeader, rf.me, "S%d reply.XTerm is %d. reply.XIndex is %d ", i, reply.XTerm, reply.XIndex)
//				_, maxIdx := rf.TermRange(reply.XTerm)
//				if maxIdx == -1 {
//					rf.nextIndex[i] = reply.XIndex
//					DebugPrintf(dLeader, rf.me, "Leader don't have XTerm %d ,S%d nextIdx -> %d", reply.XTerm, i, reply.XIndex)
//				} else {
//					rf.nextIndex[i] = maxIdx + 1
//					DebugPrintf(dLeader, rf.me, "Leader has XTerm%d ,S%d nextIdx -> %d", reply.XTerm, i, maxIdx)
//				}
//			}
//
//			// 检查计算出的 nextIndex 是否有效且在 Leader 日志范围内
//			if rf.nextIndex[i] < 0 { // 防御性检查
//				rf.nextIndex[i] = 0 // 或 1，取决于你的 dummy entry 策略
//				DebugPrintf(dError, rf.me, "S%d calculated nextIndex %d < 0. Resetting to %d.", i, rf.nextIndex[i], rf.nextIndex[i])
//			}
//			if rf.nextIndex[i] > rf.getLastLogIndex()+1 { // 修正条件
//				// 如果计算出的 nextIndex 超出了 Leader 自己的日志范围，将其限制为 Leader 日志的末尾
//				rf.nextIndex[i] = rf.getLastLogIndex() + 1
//				DebugPrintf(dError, rf.me, "S%d calculated nextIndex %d > Leader log length %d. Resetting to %d.", i, rf.nextIndex[i], rf.getLastLogIndex()+1, rf.getLastLogIndex()+1)
//			}
//
//			// 判断是否需要发送快照
//			if rf.nextIndex[i] <= rf.lastIncludedIndex {
//				DebugPrintf(dSnap, rf.me, "nextIndex < LastIncludedIndex, send snapshot to follower %d. ", i)
//				go rf.sendServerSnapshot(i)
//				rf.mu.Unlock()
//				return
//			}
//
//			nextIdx := rf.nextIndex[i]
//			prevLogIndex = nextIdx - 1
//			prevLogTerm = rf.getLogFromIndex(prevLogIndex).Term
//			entries = rf.getLogSlice(nextIdx, rf.getLastLogIndex()+1)
//
//			DebugPrintf(dLeader, rf.me, "Down S%d nextIdx, nextIdx:%d", i, rf.nextIndex[i])
//			args = AppendEntriesArgs{
//				Term:         rf.currentTerm,
//				LeaderId:     rf.me,
//				PrevLogIndex: prevLogIndex,
//				PrevLogTerm:  prevLogTerm,
//				Entries:      entries,
//				LeaderCommit: rf.commitIndex,
//			}
//			DebugPrintf(dLeader, rf.me, "Leader%d restart appendentries to Follower%d", rf.me, i)
//		}
//		rf.mu.Unlock()
//	}
//}

// 向对应server发送snapshot的函数，逻辑上是Leader执行
func (rf *Raft) sendServerSnapshot(server int) {
	//DebugPrintf(dSnap, rf.me, "Leader %d start to sendServerSnapshot... ", rf.me)
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.SnapShot,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}

	if ok := rf.sendInstallSnapshot(server, &args, &reply); !ok {
		DebugPrintf(dError, rf.me, "server snapshot send failed!")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// 如果返回的Term大于currentTerm,需要降为Follower
		DebugPrintf(dError, rf.me, "reply.Term:%d > rf.currentTerm:%d, server snapshot send failed! Down to Follower!", reply.Term, rf.currentTerm)
		if rf.state != RaftFollower {
			rf.state = RaftFollower
		}
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.persist()
		rf.ResetElectionTime()
		return
	}

	if reply.Term < rf.currentTerm {
		// 如果follower的term是低于Leader的
		DebugPrintf(dError, rf.me, "server snapshot send failed! Down to Follower! reply.Term:%d < rf.currentTerm:%d", reply.Term, rf.currentTerm)
		return
	}

	matchIdx := args.LastIncludedIndex
	nextIdx := args.LastIncludedIndex + 1

	rf.matchIndex[server] = max(matchIdx, rf.matchIndex[server])
	rf.nextIndex[server] = max(nextIdx, rf.nextIndex[server])

}

func (rf *Raft) TermRange(term int) (minIdx, maxIdx int) {
	minIdx, maxIdx = math.MaxInt, -1

	for i := rf.lastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
		if rf.getLogFromIndex(i).Term == term {
			minIdx = min(minIdx, i)
			maxIdx = max(maxIdx, i)
		}
	}
	if maxIdx == -1 {
		minIdx = -1
	}
	// 表示索引（索引从0开始）
	return minIdx, maxIdx
}

func (rf *Raft) getLogFromIndex(i int) LogEntry {

	i = i - rf.lastIncludedIndex
	if i == 0 {
		// 此时i为0说明寻求的log是lastIncludedIndex
		return LogEntry{Term: rf.lastIncludedTerm, Command: nil}
	}
	if i < 0 {
		log.Panicf("idx:%d < 0", i)
	}

	if i > len(rf.log) {
		return LogEntry{Term: -1, Command: nil}
	}
	return rf.log[i-1]
}

// 返回区间，左闭右开
func (rf *Raft) getLogSlice(left int, right int) []LogEntry {
	if left > right {
		panic("left > right")
	}
	if left < rf.lastIncludedIndex+1 || right > len(rf.log)+rf.lastIncludedIndex+1 {
		panic("left or right is out of range")
	}

	left = left - (rf.lastIncludedIndex + 1)
	right = right - (rf.lastIncludedIndex + 1)
	//newLog := make([]LogEntry, right-left)
	//copy(newLog, rf.log[left:right])
	return rf.log[left:right]
}

// 获取最新的Log的Index
func (rf *Raft) getLastLogIndex() int {
	//index := len(rf.log) - 1 + rf.lastIncludedIndex
	index := len(rf.log) + rf.lastIncludedIndex
	return index
}

// 获取LastLogTerm
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// 获取nextIndex数组中第i项对应的prevLogIndex
func (rf *Raft) getPrevLogIndex(i int) int {
	if i < 0 {
		panic("index < 0")
	}

	if i > len(rf.peers)-1 {
		panic("index out of peers!")
	}

	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIndex := rf.getPrevLogIndex(i)
	if prevLogIndex < 0 {
		panic("PrevLogIndex(i) < 0")
	} else if prevLogIndex > rf.getLastLogIndex() {
		err_msg := fmt.Sprintf("PrevLogIndex(i) > rf.getLastLogIndex(), PrevLogIndex(i) = %v, len(rf.log) = %v, nextidx: %v", rf.getPrevLogIndex(i), len(rf.log), rf.nextIndex)
		panic(err_msg)
	}
	prevLogTerm := 0
	if prevLogIndex == rf.lastIncludedIndex {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		prevLogTerm = rf.getLogFromIndex(prevLogIndex).Term
	}

	return prevLogTerm
}

func (rf *Raft) updateCommitIndex() {
	// 假定其是在加锁函数中被调用
	DebugPrintf(dCommit, rf.me, "updateCommitIndex: start to update commitIndex. matchidx : %v", rf.matchIndex)
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.getLogFromIndex(N).Term == rf.currentTerm; N-- {

		cnt := 0
		for _, idx := range rf.matchIndex {

			if idx >= N {
				cnt++
			}
		}
		if cnt > (len(rf.peers) >> 1) {
			rf.commitIndex = N
			DebugPrintf(dCommit, rf.me, "updateCommitIndex: Leader %d's commitIndex up to %d (term %d)", rf.me, rf.commitIndex, rf.currentTerm)
			break
		}
	}
}

func (rf *Raft) commitSnap(msg raftapi.ApplyMsg) {
	DebugPrintf(dCommit, rf.me, "commitSnap: Follower%d commit snapshot to server.", rf.me)
	rf.applyCh <- msg
	DebugPrintf(dSnap, rf.me, "commit Snap idx:%d-Term:%d", msg.SnapshotIndex, msg.SnapshotTerm)
}

func (rf *Raft) readPersistSnapshot(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.SnapShot = data
}

// todo： 尝试使用条件变量来进行事件驱动
//func (rf *Raft) commitMsgWithCond(applyCh chan raftapi.ApplyMsg) {
//}

func (rf *Raft) commitMsg(applyCh chan raftapi.ApplyMsg) {
	// 使用for循环来进行轮询查看
	for rf.killed() == false {
		//if rf.commitIndex-rf.lastApplied < 0 {
		//	// 防止出现新Leader导致旧Leader的lastApplied要暂时大于新commitIndex的情况
		//	// 这种时候选择continue即可,因为新Leader会保持逐步增加commitIndex和LastApplied,这些值是保持同步的
		//	continue
		//	time.Sleep(10 * time.Millisecond)
		//}
		msg := make([]raftapi.ApplyMsg, 0) // 这里申请的大小和上面if判断保持一致
		rf.mu.Lock()                       // 锁住rf对象，不让commitIndex被修改
		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
		DebugPrintf(dLog, rf.me, "Server%d start to commit", rf.me)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg = append(msg, raftapi.ApplyMsg{
				Command:      rf.getLogFromIndex(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandValid: true,
			})
		}
		rf.mu.Unlock()

		for _, v := range msg {
			applyCh <- v
			DebugPrintf(dCommit, rf.me, "commit {Index:%d , Cmd: %v} to ApplyCh", v.CommandIndex, v.Command)
		}

		time.Sleep(20 * time.Millisecond)
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble gettin     g RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	// 对server来说，检查其是否是leader，完成广播日志信息
	// start函数是一个来自应用层的接口
	// 如果不是Leader直接返回false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RaftLeader {
		isLeader = false
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command}) // 日志添加
	rf.persist()
	term = rf.currentTerm
	index = rf.getLastLogIndex()

	DebugPrintf(dClient, rf.me, "Leader%d receive log from client: %v", rf.me, rf.log)
	go rf.broadcastHeartbeat()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DebugPrintf(dWarn, rf.me, "Killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock() // 加锁保护
		if rf.state != RaftLeader && time.Now().After(rf.nextElectionTime) {
			//  进行选举

			rf.StartElection()
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		if rf.state == RaftLeader && time.Now().After(rf.nextHeartbeatTime) {
			// 发送心跳信息
			go rf.broadcastHeartbeat()
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh // 注意统一 applyCh！！！！！！
	// Your initialization code here (3A, 3B, 3C).
	// 更新状态，心跳和选举时间
	rf.ResetHeartbeat()
	rf.ResetElectionTime()

	// 更新其他状态
	rf.state = RaftFollower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0 // lab建议从log的日志从0开始
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = []LogEntry{}

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())        // 获取持久化保存
	rf.readPersistSnapshot(rf.persister.ReadSnapshot()) // 获取快照

	go rf.commitMsg(applyCh) // new 提交msg
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
