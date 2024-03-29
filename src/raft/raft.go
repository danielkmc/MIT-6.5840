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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	Entry interface{}
	Term  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Persistent state
	currentTerm       int
	votedFor          int        // candidateId that received vote in current term
	log               []LogEntry // Log entries
	offsetIndex       int        // Offset for first log entry
	lastIncludedIndex int        // Last entry index in most recent snapshot
	lastIncludedTerm  int        // Term of the last entry in the most recent snapshot

	// volatile
	commitIndex int    // Index of highest log entry known to be committed
	lastApplied int    // Index of highest log entry applied to state machine
	snapshot    []byte // Most recent snapshot

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// MUST call persist with rf lock already held!
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.offsetIndex)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int = 0
	var votedFor int = 0
	var logs []LogEntry = []LogEntry{}
	var offsetIndex int = 0
	var lastIncludedIndex int = 0
	var lastIncludedTerm int = 0
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil || d.Decode(&offsetIndex) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("[RAFT %v %v] [READ PERSIST] ERROR reading in values | currentTerm: %v | votedFor: %v | logs: %v \noffsetIndex: %v | lastIncludedIndex: %v | lastIncludedTerm: %v\n",
			rf.me, rf.currentTerm, currentTerm, votedFor, logs, offsetIndex, lastIncludedIndex, lastIncludedTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = []LogEntry{}
		rf.log = append(rf.log, logs...)
		rf.offsetIndex = offsetIndex
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.offsetIndex <= index {
		rf.lastIncludedIndex = index
		lastIncludedTerm := rf.log[index-rf.offsetIndex].Term
		rf.lastIncludedTerm = lastIncludedTerm

		newOffsetIndex := index + 1
		if len(rf.log) > newOffsetIndex-rf.offsetIndex {
			rf.log = rf.log[(newOffsetIndex - rf.offsetIndex):]
		} else {
			rf.log = []LogEntry{}
		}

		rf.offsetIndex = newOffsetIndex
		rf.snapshot = snapshot
		rf.persist()
		// Update peers if needed (nextIndex is in the snapshot)
		if rf.isLeader && rf.offsetIndex == index+1 {
			for i, nextIndex := range rf.nextIndex {
				if i == rf.me {
					continue
				}
				if nextIndex <= index {
					go rf.relaySnapshot(i)
				}
			}
		}
	}
}

// Relays current snapshot to follower server that is lagging behind. Sends an
// InstallSnapshot RPC. If the nextIndex of the server is greater than the last
// index of the most recent snapshot, then we can return true. If we are no
// longer the leader or
func (rf *Raft) relaySnapshot(server int) bool {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return false
		}
		if rf.nextIndex[server] > rf.lastIncludedIndex {
			rf.mu.Unlock()
			return true
		}
		args := InstallSnapshotArgs{
			rf.currentTerm,
			rf.me,
			rf.lastIncludedIndex,
			rf.lastIncludedTerm,
			rf.snapshot}

		rf.mu.Unlock()
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if ok {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.persist()
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if len(rf.log) > args.LastIncludedIndex-rf.offsetIndex &&
		rf.log[args.LastIncludedIndex-rf.offsetIndex].Term == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex+1-rf.offsetIndex:]
	} else {
		rf.log = []LogEntry{}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.offsetIndex = args.LastIncludedIndex + 1
	rf.snapshot = args.Data
	rf.persist()
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.mu.Unlock()
	msg := ApplyMsg{}
	msg.CommandValid = false
	msg.Snapshot = append(msg.Snapshot, args.Data...)
	msg.SnapshotIndex = args.LastIncludedIndex
	msg.SnapshotTerm = args.LastIncludedTerm
	msg.SnapshotValid = true
	rf.applyCh <- msg
}

// This function will be called by a goroutine checking for peers that are
// lagging behind and can no longer be updated since the leader doesn't have
// the entries to update their logs.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return false
	}
	if ok {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.isLeader = false
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if rf.nextIndex[server] < args.LastIncludedIndex+1 {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}

	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RPC that allows candidates to request votes from other peers whose term is
// less than ours or hasn't voted yet.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) + rf.offsetIndex - 1
		var lastLogTerm int
		if len(rf.log) == 0 {
			lastLogTerm = rf.lastIncludedTerm
		} else {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}

		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm &&
				args.LastLogIndex >= lastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
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
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return ok
	}
	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.isLeader = false
		rf.persist()
	}
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
	XTerm   int
	XIndex  int
	XLen    int
}

// RPC that handles calls to append log entries from the leader or no log
// entries and serve as a heartbeat call.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	} else {
		rf.electionTimeout = time.Now()
		leaderLogLength := rf.offsetIndex + len(rf.log)

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.isLeader = false
			rf.votedFor = -1
			rf.persist()
		}

		if args.PrevLogIndex < rf.lastIncludedIndex {
			return
		}

		// XTerm:  term in the conflicting entry (if any)
		// XIndex: index of first entry with that term (if any)
		// XLen:   log length
		// Case 1: leader doesn't have XTerm:
		// nextIndex = XIndex
		// Case 2: leader has XTerm:
		// nextIndex = leader's last entry for XTerm
		// Case 3: follower's log is too short:
		// nextIndex = XLen
		followerPrevLogIndex := args.PrevLogIndex

		if leaderLogLength <= followerPrevLogIndex ||
			(followerPrevLogIndex == rf.lastIncludedIndex &&
				rf.lastIncludedTerm != args.PrevLogTerm) ||
			(followerPrevLogIndex != rf.lastIncludedIndex &&
				rf.log[followerPrevLogIndex-rf.offsetIndex].Term != args.PrevLogTerm) {

			reply.XLen = leaderLogLength
			reply.XIndex = followerPrevLogIndex

			if leaderLogLength <= followerPrevLogIndex {
				return
			} else if followerPrevLogIndex == rf.lastIncludedIndex ||
				rf.log[followerPrevLogIndex-rf.offsetIndex].Term ==
					rf.lastIncludedTerm {
				reply.XTerm = rf.lastIncludedTerm
				reply.XIndex = rf.lastIncludedIndex
				return
			} else {
				reply.XTerm = rf.log[followerPrevLogIndex-rf.offsetIndex].Term
				for i := args.PrevLogIndex - rf.offsetIndex - 1; i >= 0; i-- {
					if rf.log[i].Term == reply.XTerm {
						reply.XIndex = i + rf.offsetIndex
					} else {
						break
					}
				}
				return
			}
		} else {
			reply.Success = true
			if len(args.Entries) > 0 {
				for i, entry := range args.Entries {
					thisIndex := args.PrevLogIndex + 1 + i - rf.offsetIndex
					if thisIndex < len(rf.log) &&
						rf.log[thisIndex].Term == entry.Term {
						continue
					} else {
						rf.log = rf.log[:thisIndex]
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}
				}
				rf.persist()
			}
		}
		// Update follower commit index to be min(LeaderCommit, last index of updated rf.log)
		if args.LeaderCommit > rf.commitIndex {
			var newCommit int = args.LeaderCommit
			if newCommit > args.PrevLogIndex+len(args.Entries) {
				newCommit = args.PrevLogIndex + len(args.Entries)
			}
			if newCommit > rf.commitIndex {
				rf.commitIndex = newCommit
			}
		}
		return
	}
}

// Handles the leader response to the reply from a follower server handling the
// AppendEntries RPC sent.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if ok {
		// If we couldn't update the follower's log, we update the nextIndex for
		// That server and try again after. We first confirm the log of the
		// follower contains the nextIndex value we sent. Then, we search for a
		// term in our log whose term matches XTerm. If there's no matching
		// term, set the follower server's nextIndex to XIndex: index of the
		// first term that has the XTerm.
		if !reply.Success && rf.nextIndex[server] == args.PrevLogIndex+1 {
			if reply.XLen <= args.PrevLogIndex {
				rf.nextIndex[server] = reply.XLen
			} else {

				matchingTermIndex := -1
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term < reply.XTerm {
						break
					}
					if rf.log[i].Term == reply.XTerm {
						matchingTermIndex = i + rf.offsetIndex
						break
					}
				}

				if matchingTermIndex == -1 {
					rf.nextIndex[server] = reply.XIndex
				} else {
					rf.nextIndex[server] = matchingTermIndex
				}
			}
		} else if reply.Success {
			if len(args.Entries) > 0 &&
				args.PrevLogIndex+1+len(args.Entries) > rf.nextIndex[server] {
				rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + rf.offsetIndex
	term := rf.currentTerm
	isLeader := rf.isLeader
	if !isLeader {
		return index, term, isLeader
	} else {
		rf.log = append(rf.log, LogEntry{command, term})
		rf.persist()
		rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.offsetIndex
		return index, term, isLeader
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.CommandIndex = rf.lastApplied
			msg.Command = rf.log[rf.lastApplied-rf.offsetIndex].Entry
			rf.mu.Unlock()
			rf.applyCh <- msg
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}

	}
}

// This go routine checks for logs to be committed to the state machine as the
// leader. The leader needs to commit when it finds a commitIndex higher than
// the current index for the current term where a majority of the other raft
// peers have the same entry for the same term.
func (rf *Raft) updateCommitIndex() {

	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		commitIndexCounts := map[int]int{}
		majorityIndex := rf.commitIndex
		majorityCondition := len(rf.peers)/2 + 1
		maxIndex := rf.commitIndex
		for _, mIndex := range rf.matchIndex {
			if mIndex > rf.commitIndex &&
				rf.log[mIndex-rf.offsetIndex].Term == rf.currentTerm {
				commitIndexCounts[mIndex]++
				if mIndex > maxIndex {
					maxIndex = mIndex
				}
			}
		}
		numServers := 0
		for maxIndex > rf.commitIndex {
			count, ok := commitIndexCounts[maxIndex]
			if ok {
				numServers += count
				if numServers >= majorityCondition {
					majorityIndex = maxIndex
				}
			}
			maxIndex--
		}

		if majorityIndex == -1 || rf.commitIndex == majorityIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		} else {
			rf.commitIndex = majorityIndex
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// Handles log replication for a single server and calls sendAppendEntries RPCs.
// If the leader detects that a server is behind, it will call
// Raft.RelaySnapshot to update the server's snapshot.
func (rf *Raft) nLogAgreement(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		leaderLastLogIndex := len(rf.log) + rf.offsetIndex - 1
		serverNextIndex := rf.nextIndex[server]
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		// Check if snapshot needs to be sent
		if serverNextIndex < rf.offsetIndex {
			rf.mu.Unlock()
			if !rf.relaySnapshot(server) {
				return
			} else {
				continue
			}
		}
		prevLogIndex := serverNextIndex - 1
		prevLogTerm := rf.lastIncludedTerm
		if prevLogIndex < rf.lastIncludedIndex {
			log.Panicf("[RAFT %v %v][LOG AGREEMENT %v] last recorded log"+
				" term for this server is less than the last entry"+
				" captured in previous snapshot! prevLogIndex: %v |"+
				" rf.lastIncludedIndex: %v\n",
				rf.me, rf.currentTerm, server, prevLogIndex,
				rf.lastIncludedIndex)
			return
		}
		if prevLogIndex != rf.lastIncludedIndex {
			prevLogTerm = rf.log[prevLogIndex-rf.offsetIndex].Term
		}
		args := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			prevLogIndex,
			prevLogTerm,
			[]LogEntry{},
			rf.commitIndex}

		if len(rf.log) != 0 && leaderLastLogIndex >= serverNextIndex {
			args.Entries = append(args.Entries,
				rf.log[serverNextIndex-rf.offsetIndex:]...)
		}

		rf.mu.Unlock()
		for !rf.killed() {
			rf.mu.Lock()
			if !rf.isLeader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok && reply.Success {
				return
			}
			time.Sleep(101 * time.Millisecond)
		}
	}
}

// Leader go routine that sends RPCs for adding and updating follower log
// entries. If there are no logs to add, then the RPC is considered a heartbeat.
func (rf *Raft) logAgreement(peers int, term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for server := range rf.nextIndex {
			if server == rf.me {
				continue
			}

			go rf.nLogAgreement(server)
		}
		rf.mu.Unlock()
		time.Sleep(101 * time.Millisecond)
	}
}

// Election logic for candidate. The candidate must receive a simple majority of
// the votes from all peers. If the term of this candidate changes at any time,
// the election is cancelled. The same is true if the electionTimeout value
// changes from the time when this election started.
func (rf *Raft) election(startTime time.Time, term int) {
	rf.mu.Lock()
	numPeers := len(rf.peers)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastIncludedIndex
	args.LastLogTerm = rf.lastIncludedTerm
	if len(rf.log) != 0 {
		args.LastLogIndex = len(rf.log) - 1 + rf.offsetIndex
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	votesReceived := 1
	voteMu := sync.Mutex{}
	voteCond := sync.NewCond(&voteMu)
	voted := 1
	var majority int = numPeers/2 + 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.currentTerm != term ||
			rf.electionTimeout.Sub(startTime).Milliseconds() != 0 {
			rf.mu.Unlock()
			return
		}

		go func(server int, candidateArgs RequestVoteArgs) {
			var reply RequestVoteReply
			for !rf.killed() {
				rf.mu.Lock()
				if rf.currentTerm != term || rf.isLeader {
					voteCond.Broadcast()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply = RequestVoteReply{}
				ok := rf.sendRequestVote(server, &candidateArgs, &reply)
				if ok {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			voteMu.Lock()
			defer voteMu.Unlock()

			if reply.VoteGranted {
				votesReceived++
			}
			voted++
			voteCond.Broadcast()
		}(i, args)
	}

	// Wait for votes to complete, a new election to start, or for the term to
	// have changed.
	voteMu.Lock()
	for votesReceived < majority && voted != numPeers {
		if rf.currentTerm != term ||
			rf.electionTimeout.Sub(startTime).Milliseconds() != 0 {
			rf.mu.Unlock()
			voteMu.Unlock()
			return
		}
		rf.mu.Unlock()
		voteCond.Wait()
		rf.mu.Lock()
	}

	if votesReceived >= majority &&
		rf.currentTerm == term &&
		rf.electionTimeout.Sub(startTime).Milliseconds() == 0 {
		rf.nextIndex = make([]int, numPeers)
		rf.matchIndex = make([]int, numPeers)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log) + rf.offsetIndex
		}
		rf.isLeader = true
		go rf.logAgreement(numPeers, rf.currentTerm)
		go rf.updateCommitIndex()
	}
	rf.mu.Unlock()
	voteMu.Unlock()
}

// Tracks the time since the last set electionTimeout value. If the time has
// exceeded the randomly set electionTimeout value betwee [500, 1300) ms, then
// this server starts an election. There is only one ticker process per server.
func (rf *Raft) ticker() {
	for !rf.killed() {
		ms := 500 + (rand.Int63() % 800)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if !rf.isLeader && time.Since(rf.electionTimeout).Milliseconds() > ms {
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()
			rf.electionTimeout = time.Now()
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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.isLeader = false
	rf.electionTimeout = time.Now()
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.offsetIndex = 0
	rf.snapshot = nil

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applyEntries()

	return rf
}
