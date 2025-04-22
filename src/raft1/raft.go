package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
		DebugPrintf(dVote, rf.me, "Follower%d update term to：%d", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 2.在相同任期的时候，若是votefor不为-1，表示已经投过票；
	// 不为args.CandidateId，表示不是由于网络原因为同一个候选人投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DebugPrintf(dVote, rf.me, "reject Vote，Follower%d have voted.", rf.me)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// --------- 开始投票 -----------
	//DebugPrintf(dVote, rf.me, "Follower%d更新当前任期为%d", rf.me, args.Term)
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	rf.ResetElectionTime()

}

// 日志信息和心跳信息的请求体
// 只有Leader可以发送
type AppendEntriesArgs struct {
	Term         int   // 候选者的任期号
	LeaderId     int   // 首领的ID
	PrevLogIndex int   //
	PrevLogTerm  int   //
	Entries      []int // log日志构成的数组
	LeaderCommit int   // Leader的commitID
}

// 日志信息和心跳信息的请求体
// 由Follower们发送
type AppendEntriesReply struct {
	Term    int  // 当前任期
	Success bool // 如果Follower们存有对应的prevLogIndex和prevLogTerm
}

// 在Lab3A中，只针对心跳信息进行发送
// 逻辑也是在Follower端执行
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 领导者任期小于当前任期
	if args.Term < rf.currentTerm {
		DebugPrintf(dVote, rf.me, "Term of Leader%d smaller than Follower%d，reject AppendEntries.", args.LeaderId, rf.me)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 领导者任期大于当前任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != RaftFollower {
			DebugPrintf(dClient, rf.me, "Receive higher Term%d, down to Follower", rf.currentTerm)
			rf.state = RaftFollower
		}
	}
	// 等于当前任期，Candidate遇到心跳信息，降为follower
	if rf.state == RaftCandidate {
		DebugPrintf(dClient, rf.me, "Candidate%d receive AppendEntries, down to Follower", rf.me)
		rf.state = RaftFollower
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	rf.ResetElectionTime()

}

func (rf *Raft) StartElection() {
	// 开始进行选举，注意每一个选举需要进行协程操作
	rf.currentTerm += 1
	rf.state = RaftCandidate
	rf.votedFor = rf.me
	DebugPrintf(dVote, rf.me, "Candidate%d start election, term：%d", rf.me, rf.currentTerm)
	go rf.broadcastElection()

	rf.ResetElectionTime()
}

// 从Candidate端进行广播请求，请求投票
func (rf *Raft) broadcastElection() {

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	voteCnt := 1
	var once sync.Once

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 每针对一个i，生成一个go协程进行投票
		go func(i int, args RequestVoteArgs) {
			reply := RequestVoteReply{}

			if !rf.sendRequestVote(i, &args, &reply) {
				// 投票过程失败，不做处理
				return
			}

			if !reply.VoteGranted {
				// 如果VoteGranted为false，表示已经投票，不做处理
				return
			}

			if reply.Term > rf.currentTerm {
				// 如果返回的Term大于自身，状态变为Follower
				rf.state = RaftFollower
				rf.votedFor = -1
				rf.currentTerm = reply.Term

				rf.ResetElectionTime()

				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 成功的情况，需要加锁，保证对voteCnt操作不会冲突

			voteCnt += 1
			if voteCnt > len(rf.peers)/2 {
				once.Do(func() {
					rf.state = RaftLeader
					DebugPrintf(dLeader, rf.me, "server%d up to Leader", rf.me)
					DebugPrintf(dTerm, rf.me, "the Term：%d", rf.currentTerm)
					// 开始广播发送AppendEntries
					rf.broadcastHeartbeat()
					rf.ResetHeartbeat()
				})
			}
		}(i, args)

		// 如果状态变为Follower，表示投票失败，直接退出循环
		if rf.state == RaftFollower {
			DebugPrintf(dError, rf.me, "server%d down to Follower，end election.", rf.me)
			break
		}
	}
}

// 进行广播心跳信息，对每一个server发送AppendEntries
func (rf *Raft) broadcastHeartbeat() {
	DebugPrintf(dLeader, rf.me, "Leader%d start to send AppendEntries, term: %d", rf.me, rf.currentTerm)
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(i int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}

			if !rf.sendAppendEntries(i, &args, &reply) {
				// 信息发送失败
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				// 接收的任期大于自身任期，降级为Follower
				DebugPrintf(dError, rf.me, "server%d receives higher term in AppendEntries reply, down to Follower", rf.me)
				rf.state = RaftFollower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.ResetElectionTime()
				return
			}

			if reply.Success {
				DebugPrintf(dLeader, rf.me, "S%d AppendEntry success.", i)
			} else {
				DebugPrintf(dLeader, rf.me, "S%d AppendEntry failed.", i)
			}

		}(i, args)

		if rf.state != RaftLeader {
			break
		}
	}
	// 更新心跳时间，如果被降为Follower，则更新选举时间
	rf.ResetHeartbeat()
	if rf.state != RaftLeader {
		rf.ResetElectionTime()
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
// if you're having trouble getting RPC to work, check that you've
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
			rf.broadcastHeartbeat()
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

	// Your initialization code here (3A, 3B, 3C).

	// 更新状态，心跳和选举时间
	rf.ResetHeartbeat()
	rf.ResetElectionTime()

	// 更新其他状态
	rf.state = RaftFollower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
