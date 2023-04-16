package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/labutil"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
func dTopicOfAppendEntriesRPC(args *AppendEntriesArgs, defaultTopic lablog.LogTopic) lablog.LogTopic {
	if len(args.Entries) == 0 {
		return lablog.Heart
	}
	return defaultTopic
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
const FOLLOWER string = "follower"
const LEADER string = "leader"
const CANDIDATE string = "candidate"

const heartInterval = 100

const electionTimeoutMax = 1200
const electionTimeoutMin = 800

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state //?什么时候会出现共享访问
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// for all servers, persistent state
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// for all servers,volatile state
	CommitIndex   int
	LastApplied   int
	ElectionAlarm time.Time // election timer
	State         string    // leader, follower, candidate
	CommitTrigger chan bool // trigger commit

	// for leader only,volatile state
	NextIndex         []int
	MatchIndex        []int
	AppendEntriesChan []chan int //用于leader等待follower的回复
	LastIncludedIndex int
	LastIncludedTerm  int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (e LogEntry) String() string {
	commandStr := fmt.Sprintf("%v", e.Command)
	if len(commandStr) > 15 {
		commandStr = commandStr[:15]
	}
	return fmt.Sprintf("{I:%d T:%d C:%s}", e.Index, e.Term, commandStr)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) raftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil ||
		e.Encode(rf.LastIncludedIndex) != nil ||
		e.Encode(rf.LastIncludedTerm) != nil {
		return nil
	}
	return w.Bytes()
}
func (rf *Raft) persist() {
	if data := rf.raftState(); data == nil {
		lablog.Debug(rf.me, lablog.Error, "Write persistence failed")
	} else {
		lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
		lablog.Debug(rf.me, lablog.Persist, "Saved state T:%d VF:%d, (LII:%d LIT:%d), (LLI:%d LLT:%d)", rf.CurrentTerm, rf.VotedFor, rf.LastIncludedIndex, rf.LastIncludedTerm, lastLogIndex, lastLogTerm)
		rf.persister.SaveRaftState(data)
	}

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {

	if len(data) == 0 { // bootstrap without any state
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm, VotedFor, LastIncludedIndex, LastIncludedTerm int
	var Logs []LogEntry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Logs) != nil ||
		d.Decode(&LastIncludedIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil {
		lablog.Debug(rf.me, lablog.Error, "Read broken persistence")
		return
	}
	rf.CurrentTerm = CurrentTerm
	rf.VotedFor = VotedFor
	rf.Log = Logs
	rf.LastIncludedIndex = LastIncludedIndex
	rf.LastIncludedTerm = LastIncludedTerm
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term int

	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	//1. Reply false if term < currentTerm 2. candidateId is me 3.If killed
	if args.Term < rf.CurrentTerm || args.CandidateId == rf.me || rf.killed() {
		return

	}
	if args.Term > rf.CurrentTerm {
		rf.toFollower(args.Term)

	}
	//每个server只能投一票，投完后更新ElectionAlarm
	//? 如果在下属函数中使用toFollower，会有什么问题？
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && !rf.ifMyLogMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {

		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true

		rf.ElectionAlarm = nextElectionAlarm()

		rf.persist()
	}

	// Your code here (2A).

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.State != LEADER || rf.killed() {
		return
	}
	index = rf.NextIndex[rf.me]
	isLeader = true
	rf.Log = append(rf.Log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.NextIndex[rf.me]++
	rf.MatchIndex[rf.me] = index
	lablog.Debug(rf.me, lablog.Log2, "Received log: %v, with NI:%v, MI:%v", rf.Log[len(rf.Log)-1], rf.NextIndex, rf.MatchIndex)
	rf.persist()
	for i, c := range rf.AppendEntriesChan {
		if i != rf.me {
			select {
			case c <- 0:
			default:
			}
		}
	}
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// to terminate all long-run goroutines
	// quit entriesAppender
	for _, c := range rf.AppendEntriesChan {
		if c != nil {
			close(c)
		}
	}
	// IMPORTANT: not just close channels, but also need to reset appendEntriesCh to avoid send to closed channel
	rf.AppendEntriesChan = nil

	// quit snapshotInstaller
	// for _, c := range rf.installSnapshotCh {
	// 	if c != nil {
	// 		close(c)
	// 	}
	// }
	// // IMPORTANT: not just close channels, but also need to reset installSnapshotCh to avoid send to closed channel
	// rf.installSnapshotCh = nil

	// quit committer
	if rf.CommitTrigger != nil {
		close(rf.CommitTrigger)
	}
	// IMPORTANT: not just close channels, but also need to reset CommitTrigger to avoid send to closed channel
	rf.CommitTrigger = nil

	// quit snapshoter
	// close(rf.snapshotTrigger)
	// // IMPORTANT: not just close channels, but also need to reset snapshotTrigger to avoid send to closed channel
	// rf.snapshotTrigger = nil
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var sleepDuration time.Duration
	for !rf.killed() {

		rf.mu.Lock()
		//
		if rf.State == LEADER {
			rf.ElectionAlarm = nextElectionAlarm()
			sleepDuration = time.Until(rf.ElectionAlarm)
			rf.mu.Unlock()
		} else {
			lablog.Debug(rf.me, lablog.Timer, "Not Leader, checking election timeout")
			if rf.ElectionAlarm.After(time.Now()) {
				sleepDuration = time.Until(rf.ElectionAlarm)
				rf.mu.Unlock()
			} else {
				//如果当前时间大于ElectionAlarm，那么就开始竞选

				rf.CurrentTerm++ //? 如果竞选失败，还会再加回来吗？
				term := rf.CurrentTerm
				lablog.Debug(rf.me, lablog.Term, "Converting to Candidate, calling election T:%d", term)
				rf.VotedFor = rf.me
				rf.State = CANDIDATE
				rf.persist()

				lablog.Debug(rf.me, lablog.Timer, "Resetting ELT because election")
				rf.ElectionAlarm = nextElectionAlarm()
				sleepDuration = time.Until(rf.ElectionAlarm)

				rf.mu.Unlock()

				//send request vote
				var args = &RequestVoteArgs{}
				args.Term = rf.CurrentTerm
				args.CandidateId = rf.me

				//如果日志为空，那么就是0
				args.LastLogIndex, args.LastLogTerm = rf.lastLogIndexAndTerm()

				//grant用于收集投票
				grant := make(chan bool)
				// var reply RequestVoteReply
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.pre_sendRequestVote(term, i, args, grant)
					}
				}

				go rf.collectVote(args.Term, grant)

			}
		}
		lablog.Debug(rf.me, lablog.Timer, "Ticker going to sleep for %d ms", sleepDuration.Milliseconds())
		time.Sleep(sleepDuration)

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 0)

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.State = FOLLOWER
	rf.ElectionAlarm = time.Now().Add(time.Duration(labutil.RandRange(0, electionTimeoutMax-electionTimeoutMin)) * time.Millisecond)

	rf.NextIndex = nil
	rf.MatchIndex = nil
	rf.AppendEntriesChan = nil
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0

	rf.CommitTrigger = make(chan bool, 1)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()

	lablog.Debug(rf.me, lablog.Client, "Started at T:%d with (LII:%d LIT:%d), (LLI:%d LLT:%d)", rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm, lastLogIndex, lastLogTerm)
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commiter(applyCh, rf.CommitTrigger)
	return rf
}

func (rf *Raft) commiter(applyCh chan ApplyMsg, triggerCh chan bool) {

	defer func() {
		// IMPORTANT: close channel to avoid resource leak
		close(applyCh)
		// IMPORTANT: drain CommitTrigger to avoid goroutine resource leak
		for i := 0; i < len(triggerCh); i++ {
			<-triggerCh
		}
	}()

	for !rf.killed() {
		isCommit, ok := <-triggerCh

		if !ok {
			return
		}
		rf.mu.Lock()
		if !isCommit {
			// re-enable commitTrigger to be ready to accept commit signal
			rf.CommitTrigger = triggerCh
			// is received snapshot from leader
			data := rf.persister.ReadSnapshot()
			if rf.LastIncludedIndex == 0 || len(data) == 0 {
				// snapshot data invalid
				rf.mu.Unlock()
				continue
			}

			// send snapshot back to upper-level service
			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      data,
				SnapshotIndex: rf.LastIncludedIndex,
				SnapshotTerm:  rf.LastIncludedTerm,
			}
			rf.mu.Unlock()

			applyCh <- applyMsg
			continue
		}
		//如果是commit，那么就把log中的command发送到applyCh中
		for rf.CommitTrigger != nil && rf.LastApplied < rf.CommitIndex {
			rf.LastApplied++
			//在这里加锁，是因为在这里会有多个goroutine同时访问applyCh
			logEntry := rf.Log[rf.LastApplied-1]
			//? 为什么要减1
			lablog.Debug(rf.me, lablog.Client, "CI:%d > LA:%d, apply log: %s", rf.CommitIndex, rf.LastApplied-1, logEntry)
			rf.mu.Unlock()
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}

			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) toFollower(term int) {
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.State = FOLLOWER

	for _, c := range rf.AppendEntriesChan {
		if c != nil {
			close(c)
		}
	}
	rf.AppendEntriesChan = nil
	rf.persist()
	rf.NextIndex = nil
	rf.MatchIndex = nil
}
func (rf *Raft) ifMyLogMoreUpToDate(index int, term int) bool {
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date
	myindex, myterm := 0, 0
	if l := len(rf.Log); l > 0 {
		myindex, myterm = rf.Log[l-1].Index, rf.Log[l-1].Term
	}

	if myterm != term {
		return myterm > term
	}
	return myindex > index
}
func (rf *Raft) pre_sendRequestVote(term int, server int, args *RequestVoteArgs, grant chan bool) {

	//Call函数用法？
	granted := false
	//确保grant一定会被写入
	defer func() { grant <- granted }()

	rf.mu.Lock()
	//在这里判断是否已经成为leader.如果是，就不用再投票了 //? 自己的term发生变化，就不用投票了
	if rf.State != CANDIDATE || rf.killed() || args.Term != rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	reply := &RequestVoteReply{}
	ret := rf.sendRequestVote(server, args, reply)
	//drop reply

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != CANDIDATE || rf.killed() {
		return
	}
	if rf.CurrentTerm != term {
		return
	}
	if !ret {
		lablog.Debug(rf.me, lablog.Drop, "-> S%d RV been dropped: {T:%d LLI:%d LLT:%d}", server, args.Term, args.LastLogIndex, args.LastLogTerm)
		return
	}
	if reply.Term > rf.CurrentTerm {

		lablog.Debug(rf.me, lablog.Term, "RV <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.CurrentTerm)
		rf.toFollower(reply.Term)
		return
	}

	if rf.CurrentTerm != term {
		return
	}
	granted = reply.VoteGranted
	lablog.Debug(rf.me, lablog.Vote, "<- S%d Got Vote: %t, at T%d", server, granted, term)

}
func (rf *Raft) lastLogIndexAndTerm() (index, term int) {
	index = 0
	term = 0
	if l := len(rf.Log); l > 0 {
		index, term = rf.Log[l-1].Index, rf.Log[l-1].Term
	}
	return index, term
}
func (rf *Raft) collectVote(term int, grant chan bool) {

	//收集投票
	vote := 1

	//排除自己
	done := false
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-grant {
			vote++
		}
		//如果获得的票数大于一半，那么就成为leader
		if !done && vote > len(rf.peers)/2 {
			done = true
			rf.mu.Lock()

			//如果状态不是candidate，那么就不成为leader
			if rf.State != CANDIDATE || rf.CurrentTerm != term || rf.killed() {
				rf.mu.Unlock()
			} else {
				//成为leader
				rf.State = LEADER

				rf.NextIndex = make([]int, len(rf.peers))
				rf.MatchIndex = make([]int, len(rf.peers))
				lastlogIndex, _ := rf.lastLogIndexAndTerm()
				for i := 0; i < len(rf.peers); i++ {
					rf.NextIndex[i] = lastlogIndex + 1 //?为什么要加1,论文规定
					rf.MatchIndex[i] = 0               // safe to initialize to 0
				}
				rf.MatchIndex[rf.me] = lastlogIndex

				rf.AppendEntriesChan = make([]chan int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						rf.AppendEntriesChan[i] = make(chan int)
						go rf.entriesAppender(i, rf.AppendEntriesChan[i], rf.CurrentTerm)
						// TODO go rf.sendAppendEntries(i)
					}
				}
				go rf.leaderTicker(rf.CurrentTerm)
				lablog.Debug(rf.me, lablog.Leader, "Achieved Majority for T%d, converting to Leader, NI:%v, MI:%v", rf.CurrentTerm, rf.NextIndex, rf.MatchIndex)
				rf.mu.Unlock()

			}

		}
	}

}

func (rf *Raft) leaderTicker(term int) {

	for !rf.killed() {
		rf.mu.Lock()

		if rf.State != LEADER || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return
		}

		//每隔ms发送一次心跳
		lablog.Debug(rf.me, lablog.Timer, "Leader at T%d, broadcasting heartbeats", term)
		for i, c := range rf.AppendEntriesChan {
			if i != rf.me {
				select {
				case c <- 0:
				default:
				}
			}

		}
		rf.mu.Unlock()

		time.Sleep(heartInterval * time.Millisecond)

	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for optimization
	ConflictTerm  int
	ConflictIndex int
	XLen          int // log length
}

func (rf *Raft) constructAppenderArgs(server int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.CommitIndex

	//新日志的前一条日志的index
	args.PrevLogIndex = rf.NextIndex[server] - 1
	// prevLogIndex := rf.nextIndex[server] - 1
	args.PrevLogTerm = 0

	if i := args.PrevLogIndex - 1; i > -1 {
		args.PrevLogTerm = rf.Log[i].Term
	}

	var entries []LogEntry
	//leader日志的最后一条日志的index比follower的小，那么就不用发送日志了
	if lastlogIndex, _ := rf.lastLogIndexAndTerm(); lastlogIndex <= args.PrevLogIndex {
		entries = nil
	} else if args.PrevLogIndex >= 0 {

		newEntries := rf.Log[args.PrevLogIndex:]
		entries = make([]LogEntry, len(newEntries))
		// avoid data-race
		copy(entries, newEntries)
	}

	// args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
	args.Entries = entries

	return args
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ret := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ret
}
func intentOfAppendEntriesRPC(args *AppendEntriesArgs) string {
	if len(args.Entries) == 0 {
		return "HB"
	}
	return "AE"
}

/********************** AppendEntries RPC handler *****************************/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.CurrentTerm

	//如果收到的term比自己的term小，那么就拒绝
	if args.Term < rf.CurrentTerm || args.LeaderId == rf.me || rf.killed() {
		return
	}
	//如果收到的term比自己的term大，那么就转换成follower
	if args.Term > rf.CurrentTerm {
		lablog.Debug(rf.me, lablog.Term, "S%d %s request term is higher(%d > %d), following", args.LeaderId, intentOfAppendEntriesRPC(args), args.Term, rf.CurrentTerm)
		rf.toFollower(args.Term)
	}

	// 可能是上一步的 stepDown 造成的，也可能是自己本身就是 Candidate
	if rf.State == CANDIDATE && args.Term >= rf.CurrentTerm {
		lablog.Debug(rf.me, lablog.Term, "I'm Candidate, S%d %s request term %d >= %d, following", args.LeaderId, intentOfAppendEntriesRPC(args), args.Term, rf.CurrentTerm)
		rf.toFollower(args.Term)
	}
	//收到AE后，重置选举超时时间
	rf.ElectionAlarm = nextElectionAlarm()

	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	//如果收到的AE中的prevLogIndex比自己的log的最后一个index还大，那么就拒绝，并且返回冲突的index
	if lastLogIndex < args.PrevLogIndex {
		reply.XLen = lastLogIndex + 1
		// reply.ConflictIndex = lastLogIndex + 1
		return
	}

	//如果相同的index的term不同，那么就拒绝，并且返回冲突的term
	// prevLogTerm := rf.Log[args.PrevLogIndex].Term
	var prevLogTerm int
	switch {
	case args.PrevLogIndex == 0:
		prevLogTerm = 0
	case args.PrevLogIndex < 0:
		// follower has committed PrevLogIndex log =>
		// PrevLogIndex consistency check is already done =>
		// it's OK to start log consistency check at 0
		args.PrevLogIndex = 0
		prevLogTerm = 0
		// trim args.Entries to start after LastIncludedIndex
		sameEntryInArgsEntries := false
		for i := range args.Entries {
			if args.Entries[i].Index == 0 && args.Entries[i].Term == 0 {
				sameEntryInArgsEntries = true
				args.Entries = args.Entries[i+1:]
				break
			}
		}
		if !sameEntryInArgsEntries {
			// not found LastIncludedIndex log entry in args.Entries =>
			// args.Entries are all covered by LastIncludedIndex =>
			// args.Entries are all committed =>
			// safe to trim args.Entries to empty slice
			args.Entries = make([]LogEntry, 0)
		}
	default:
		// args.PrevLogIndex > rf.LastIncludedIndex
		prevLogTerm = rf.Log[args.PrevLogIndex-1].Term
	}

	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm

		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			reply.ConflictIndex = rf.Log[i].Index
			if rf.Log[i].Term != prevLogTerm {

				break
			}
		}
		return
	}
	reply.Success = true
	//如果收到的AE中的entries不为空，那么就更新自己的log
	if len(args.Entries) > 0 {
		//从prevLogIndex开始，向后查找，找到第一个不同的entry
		lablog.Debug(rf.me, lablog.Info, "Received: %v from S%d at T%d", args.Entries, args.LeaderId, args.Term)
		log_from_prev := rf.Log[args.PrevLogIndex:]
		var i int
		needsave := false
		for i = 0; i < len(args.Entries) && i < len(log_from_prev); i++ {
			if args.Entries[i].Term != log_from_prev[i].Term {
				//如果不同，那么就删除自己的log中从prevLogIndex开始的所有entry
				rf.Log = rf.Log[:args.PrevLogIndex+i]
				needsave = true
				break
			}
		}
		//两种情况，一种是从prevLogIndex开始，自己的log和收到的log完全一样，那么就直接追加收到的log
		//一种情况是从prevLogIndex开始，存在不同的项，但是已经在上面删除了，那么就直接追加收到的log
		if i < len(args.Entries) {
			lablog.Debug(rf.me, lablog.Info, "Append new: %v from i: %d", args.Entries[i:], i)
			rf.Log = append(rf.Log, args.Entries[i:]...)
			needsave = true
		}
		//如果日志有更新，那么就持久化
		if needsave {
			rf.persist()
		}

	}

	if args.LeaderCommit > rf.CommitIndex {
		lastLogIndex, _ := rf.lastLogIndexAndTerm()
		rf.CommitIndex = labutil.Min(args.LeaderCommit, lastLogIndex)
		select {
		case rf.CommitTrigger <- true:
		default:
		}
	}

}
func (rf *Raft) updateCommitIndex(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != LEADER || rf.CurrentTerm != term || rf.killed() {
		return
	}

	//论文中的伪代码
	//for N = commitIndex + 1 to lastLogIndex
	//if N > commitIndex and a majority of matchIndex[i] ≥ N and log[N].term == currentTerm
	//commitIndex = N
	//applyCond.Signal()
	//end for
	//这里的N是从commitIndex+1开始的，所以要先加1
	// OldCommitIndex := rf.CommitIndex
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	if rf.CommitIndex >= lastLogIndex {
		return
	}
	oldCommitIndex := rf.CommitIndex

	for N := rf.CommitIndex + 1; N < len(rf.Log)+1; N++ {
		if rf.Log[N-1].Term == term {
			//这里的matchIndex是从0开始的，所以要加1
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.MatchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				lablog.Debug(rf.me, lablog.Commit, "Commit achieved majority, set CI from %d to %d", rf.CommitIndex, N)
				rf.CommitIndex = N
				// rf.applyCond.Signal()
			}
		}

	}

	if oldCommitIndex != rf.CommitIndex {
		// going to commit
		select {
		case rf.CommitTrigger <- true:
		default:
		}
	}
}
func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs, term int, serialNo int) {

	reply := &AppendEntriesReply{}
	ret := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != LEADER || rf.CurrentTerm != term || rf.killed() {
		return
	}
	rpcIntent := intentOfAppendEntriesRPC(args)
	if !ret {
		if rpcIntent == "HB" {
			// OPTIMIZATION: don't retry heartbeat rpc
			return
		}
		//一开始给args.PrevlogIndex赋值时，是rf.nextIndex[server]-1，这里判断是不是小于这个值，如果是，说明这个RPC已经过时了，不需要重试
		if args.PrevLogIndex < rf.NextIndex[server]-1 {
			// OPTIMIZATION: this AppendEntries RPC is out-of-data, don't retry
			return
		}

		// retry when no reply from the server
		select {
		//申请重发
		case rf.AppendEntriesChan[server] <- serialNo: // retry with serialNo
			lablog.Debug(rf.me, lablog.Drop, "-> S%d %s been dropped: {T:%d PLI:%d PLT:%d LC:%d log length:%d}, retry", server, rpcIntent, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
			// lablog.Debug(rf.me, lablog.Drop, "-> S%d %s been dropped: {T:%d PLI:%d PLT:%d LC:%d log length:%d}, retry", server, rpcIntent, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
		default:
		}
		return

	}
	// lablog.Debug(rf.me, lablog.Info, "%s <- S%d Reply: %+v", rpcIntent, server, *reply)
	lablog.Debug(rf.me, dTopicOfAppendEntriesRPC(args, lablog.Log), "%s <- S%d Reply: %+v", rpcIntent, server, *reply)
	//如果答复的server的term大于自己的term，那么就变成follower
	if reply.Term > rf.CurrentTerm {
		lablog.Debug(rf.me, lablog.Term, "%s <- S%d Term is higher(%d > %d), following", rpcIntent, server, reply.Term, rf.CurrentTerm)
		rf.toFollower(reply.Term)
		rf.ElectionAlarm = nextElectionAlarm()
		return
	}
	//可能是上一步发送chanshen，所以要再判断一下
	if rf.CurrentTerm != term {
		return
	}
	if reply.Success {
		//如果成功，那么就更新NextIndex和MatchIndex
		//args.Entries被发送的时候，是从rf.nextIndex[server]开始的，所以这里要加上len(args.Entries)

		rf.NextIndex[server] = labutil.Max(args.PrevLogIndex+len(args.Entries)+1, rf.NextIndex[server])
		rf.MatchIndex[server] = labutil.Max(args.PrevLogIndex+len(args.Entries), rf.MatchIndex[server])
		lablog.Debug(rf.me, dTopicOfAppendEntriesRPC(args, lablog.Log), "%s RPC -> S%d success, updated NI:%v, MI:%v", rpcIntent, server, rf.NextIndex, rf.MatchIndex)
		// lablog.Debug(rf.me, lablog.Info, "%s RPC -> S%d success, updated NI:%v, MI:%v", rpcIntent, server, rf.NextIndex, rf.MatchIndex)
		// rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		// rf.MatchIndex[server] = rf.NextIndex[server] - 1
		go rf.updateCommitIndex(term)

		//及时ruturn
		return
	}

	if reply.XLen != 0 && reply.ConflictTerm == 0 {
		// follower's log is too short
		rf.NextIndex[server] = reply.XLen
	} else {
		var entryIndex, entryTerm int
		for i := len(rf.Log) - 1; i >= -1; i-- {
			if i < 0 {
				entryIndex, entryTerm = rf.lastLogIndexAndTerm()
			} else {
				entryIndex, entryTerm = rf.Log[i].Index, rf.Log[i].Term
			}

			if entryTerm == reply.ConflictTerm {
				// leader's log has ConflictTerm
				rf.NextIndex[server] = entryIndex + 1
				break
			}
			if entryTerm < reply.ConflictTerm {
				// leader's log doesn't have ConflictTerm
				rf.NextIndex[server] = reply.ConflictIndex
				break
			}

		}
	}

	select {
	case rf.AppendEntriesChan[server] <- 0: // retry with serialNo
		// lablog.Debug(rf.me, lablog.Drop, "-> S%d %s been dropped: {T:%d PLI:%d PLT:%d LC:%d log length:%d}, retry", server, rpcIntent, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	default:
		// lablog.Debug(rf.me, lablog.Drop, "-> S%d %s been dropped: {T:%d PLI:%d PLT:%d LC:%d log length:%d}, retry", server, rpcIntent, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	}

}

// 每个entriesAppender都是一个goroutine,用于处理AppendEntries RPC
// ch<-int是用于通知该goroutine有新的AppendEntries RPC要发送
func (rf *Raft) entriesAppender(server int, ch <-chan int, term int) {
	i := 1 //用于记录发送的次数

	for !rf.killed() {
		//等待新的AppendEntries RPC
		serialNo, ok := <-ch
		if !ok {
			return //如果channel已经关闭，那么就退出
		}
		rf.mu.Lock()

		//如果term发生变化，或者不是Leader 那么就不用发送了
		if rf.State != LEADER || rf.CurrentTerm != term || rf.killed() {
			rf.mu.Unlock()
			return
		}
		args := rf.constructAppenderArgs(server)

		rf.mu.Unlock()

		if serialNo == 0 || serialNo >= i {
			go rf.appendEntries(server, args, term, i)
			i++ //发送次数加1
		}

	}
}

func nextElectionAlarm() time.Time {
	return time.Now().Add(time.Duration(labutil.RandRange(electionTimeoutMin, electionTimeoutMax)) * time.Millisecond)
}
