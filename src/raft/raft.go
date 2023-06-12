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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type LogEntry struct {
	Entry int
	Term  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // -1 is null/none
	log         []LogEntry

	// volatile
	commitIndex int
	lastApplied int

	// volatile for leaders
	nextIndex  []int
	matchIndex []int

	// election timeout
	electionTimeout time.Time
	isLeader        bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
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
	// Your code here (2C).
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
	// Your code here (2C).
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
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Set reply values
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// If requester term is less than our term, return
	if args.Term < rf.currentTerm {
		fmt.Printf("[Raft %v] [REQUESTVOTE] term not higher, no vote! arg: %v | me: %v\n", rf.me, args.Term, rf.currentTerm)
		return
	}
	// update our term if it's lower and reset who we voted for
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// at least as up-to-date : compare index and term
		// if the requester has the larger latest term
		// OR they have the same latest term but the requester has a longer log
		// give vote
		// fmt.Printf("[Raft %v] [REQUESTVOTE] haven't voted or already voted for the same candidate\n", rf.me)
		lastLogTerm := rf.log[rf.commitIndex].Term
		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.commitIndex) {
			// grant vote
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			// reset timeout only if we granted vote
			rf.electionTimeout = time.Now()
		}
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
	rf.mu.Lock()
	// if the reply term is greater, WE ARE NOT LEADER
	if ok && reply.Term > rf.currentTerm {
		// response indicates we're not the latest
		// stop election for this server and update term
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.isLeader = false
	}
	rf.mu.Unlock()
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Print("[RAFT] Entering append entries...\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// only true if follower contained entry matching prevLogIndex and prevLogTerm
	reply.Success = false
	// Our term is greater, don't change anything
	if args.Term < rf.currentTerm {
		return
	} else if args.Term >= rf.currentTerm {
		// fmt.Printf("[Raft %v] [APPEND ENTRIES] term condition met!\n", rf.me)
		// Cancel existing election
		// Update our term if we find a higher term or same term (this should stop eelection)
		rf.electionTimeout = time.Now()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.isLeader = false
			rf.votedFor = -1
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Update term whenever we can
	// Tells the server that we are not the leader (no more heartbeats)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.isLeader = false
		rf.votedFor = -1
	}
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

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Ensures that the leader and other followers do not start elections.
func (rf *Raft) heartbeat(peers int, term int) {
	// rf.mu.Lock()
	// myself := rf.me
	// rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		// myself := rf.me
		// check if we're still the leader
		if !rf.isLeader {
			// revert to follower state
			fmt.Printf("[Raft %v] [HEARTBEAT] lost leader status\n", rf.me)
			rf.mu.Unlock()
			return
		}
		// fmt.Printf("[Raft %v] [HEARTBEAT] sending heartbeat\n", myself)
		args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term, []LogEntry{}, rf.commitIndex}
		rf.mu.Unlock()
		for i := 0; i < peers; i++ {
			// parallel heartbeats
			rf.mu.Lock()
			if !rf.isLeader {
				// stop heartbeats if no longer leader
				rf.mu.Unlock()
				return
			}
			fmt.Printf("[Raft %v] [HEARTBEAT %v] sending heartbeat to SERVER %v\n", rf.me, rf.currentTerm, i)
			rf.mu.Unlock()

			go func(server int) {
				var reply AppendEntriesReply
				// send indefinitely until response obtained
				for {
					reply = AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok {
						break
					}
				}
			}(i)
		}
		// 10 heartbeats per second max -> minimum of 100 ms timeout
		ms := 101
		// fmt.Printf("[Raft %v] [HEARTBEAT] sending heartbeat\n", myself)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) election(startTime time.Time, term int) {
	// Elect for myself
	// increment
	rf.mu.Lock()
	peers := len(rf.peers)
	myself := rf.me
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term}
	rf.votedFor = myself
	rf.mu.Unlock()
	count := 1
	voteMu := sync.Mutex{}
	voteCond := sync.NewCond(&voteMu)
	finished := 1
	var majority int = peers/2 + 1
	for i := 0; i < peers; i++ {
		// check if we're still a candidate
		if i == myself {
			continue
		}
		rf.mu.Lock()
		// check if we're still the candidate or if a new election started or
		// leader has been selected already
		if rf.currentTerm != term || rf.electionTimeout.Sub(startTime).Microseconds() > 0 {
			// end election and get out if we're no longer a candidate
			// or we started a new election
			fmt.Printf("[Raft %v] [ELECTION %v] no longer candidate or new election %v started!\n", myself, term, rf.currentTerm)
			fmt.Printf("start time %v | election time %v\n", startTime, rf.electionTimeout)
			rf.mu.Unlock()
			return
		}
		fmt.Printf("[Raft %v] [ELECTION %v] sending RequestVote to server %v!\n", myself, term, i)
		rf.mu.Unlock()
		// send votes for all peers
		go func(server int, candidateArgs RequestVoteArgs) {
			var reply RequestVoteReply
			// retry indefinitely
			for {
				reply = RequestVoteReply{}
				ok := rf.sendRequestVote(server, &candidateArgs, &reply)
				if ok {
					break
				}
			}
			voteMu.Lock()
			defer voteMu.Unlock()
			// if ok, it means we're still a candidate
			if reply.VoteGranted {
				count++
			}
			fmt.Printf("[Raft %v] [ELECTION %v] vote received from server %v!\n", myself, term, server)
			finished++
			voteCond.Broadcast()
		}(i, args)

	}

	// Only reaches here if election is still the same
	voteMu.Lock()
	for count < majority && finished != peers {
		fmt.Printf("[Raft %v] [ELECTION %v]waiting on condition | count: %v, majority: %v | finished: %v, peers: %v \n", myself, term, count, majority, finished, peers)
		voteCond.Wait()
	}
	fmt.Printf("[Raft %v] [ELECTION %v] outside loop | %v %v | %v %v\n", myself, term, count, majority, finished, peers)
	// If we have a quorum, then we can be selected as leader!
	if count >= majority {
		rf.mu.Lock()
		if rf.currentTerm != term || rf.electionTimeout.Sub(startTime).Microseconds() > 0 {
			fmt.Printf("[Raft %v] [ELECTION %v] term changed, invalid election: %v -> %v\n", myself, term, term, rf.currentTerm)
		} else {
			// won election
			// start heartbeat process
			fmt.Printf("[Raft %v] [ELECTION %v] WON ELECTION, received %v of %v majority votes!\n", myself, term, count, peers)
			rf.nextIndex = make([]int, peers)
			rf.matchIndex = make([]int, peers)
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.commitIndex + 1
			}
			rf.isLeader = true
			go rf.heartbeat(peers, term)
		}
		rf.mu.Unlock()
	} else {
		fmt.Printf("[Raft %v] [ELECTION] majority not reached, lost election | %v of %v\n", myself, count, peers)
	}
	voteMu.Unlock()
}

// How elections are started
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// increased since tester limits to 10 heartbeats per second
		// 300 - 600 ms
		ms := 350 + (rand.Int63() % 350)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//
		// During this time, the election timeout can be altered by
		// AppendEntries
		// RequestVote
		//
		rf.mu.Lock()
		// After sleeping, if the electionTimeout value is more than ms away
		// it means it was not altered
		// if we're the leader, we won't need to check electionTimeout
		if !rf.isLeader && time.Since(rf.electionTimeout).Milliseconds() >= ms {
			fmt.Printf("[Raft %v] [TICKER] time since: %v / %v\n", rf.me, time.Since(rf.electionTimeout).Milliseconds(), ms)
			// reset election timer
			rf.currentTerm += 1
			rf.electionTimeout = time.Now()
			fmt.Printf("[Raft %v] [TICKER] starting election...%v\n", rf.me, rf.currentTerm)
			go rf.election(rf.electionTimeout, rf.currentTerm)
		}
		rf.mu.Unlock()
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

	fmt.Printf("[Raft %v] starting up...\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.isLeader = false
	rf.electionTimeout = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// ticker -> election -> heartbeats
	go rf.ticker()

	return rf
}
