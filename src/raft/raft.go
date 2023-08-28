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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	// snapshotMu   sync.Mutex
	// snapshotCond sync.Cond
	// snapshotting bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm       int
	votedFor          int // -1 is null/none
	log               []LogEntry
	offsetIndex       int
	lastIncludedIndex int
	lastIncludedTerm  int

	// volatile
	commitIndex int
	lastApplied int
	snapshot    []byte

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

// MUST call persist with rf lock already held!
func (rf *Raft) persist() {
	// Your code here (2C).

	// Persistent state:
	// currentTerm
	// votedFor
	// log[]
	// offsetIndex

	// Example:
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
	// Don't need to lock here since we're starting up the Raft, so this won't be ran concurrently
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var offsetIndex int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil || d.Decode(&offsetIndex) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("[RAFT %v %v] [READ PERSIST] ERROR reading in values | currentTerm: %v | votedFor: %v | logs: %v \noffsetIndex: %v | lastIncludedIndex: %v | lastIncludedTerm: %v\n",
			rf.me, rf.currentTerm, currentTerm, votedFor, logs, offsetIndex, lastIncludedIndex, lastIncludedTerm)
	} else {
		DPrintf("[RAFT %v] [READ PERSIST] READ IN STATE!\n", rf.me)
		DPrintf("currentTerm: %v | votedFor: %v | log: %v | offsetIndex: %v\n", currentTerm, votedFor, logs, offsetIndex)
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
	// Your code here (2D).
	// every peer will have this function called on them regardless if they are a leader

	// index = the first index the log should store
	// offsetIndex will be equal to index since the first entry is 0, so index + 0 = index, the first index of the log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.offsetIndex <= index {
		DPrintf("[RAFT %v %v][SNAPSHOT] received snapshot with index %v!\n", rf.me, rf.currentTerm, index)
		DPrintf("offsetIndex: %v | len(rf.log): %v\nlog: %v\n", rf.offsetIndex, len(rf.log), rf.log)
		rf.lastIncludedIndex = index
		lastIncludedTerm := rf.log[index-rf.offsetIndex].Term
		rf.lastIncludedTerm = lastIncludedTerm

		newOffsetIndex := index + 1
		if len(rf.log) > newOffsetIndex-rf.offsetIndex {
			// DPrintf("[RAFT %v %v][SNAPSHOT] starting index: %v\n", rf.me, rf.currentTerm, newOffsetIndex-rf.offsetIndex)
			rf.log = rf.log[(newOffsetIndex - rf.offsetIndex):]
		} else {
			rf.log = []LogEntry{}
		}
		rf.offsetIndex = newOffsetIndex
		DPrintf("[RAFT %v %v][SNAPSHOT] new log length: %v\n", rf.me, rf.currentTerm, len(rf.log))
		DPrintf("log: %v\n", rf.log)

		// if index > rf.commitIndex {
		// 	DPrintf("[RAFT %v %v][SNAPSHOT] setting commitIndex to %v! lastApplied previously: %v\n", rf.me, rf.currentTerm,
		// 		index, rf.lastApplied)
		// 	// Triggers raft to send msgs on applyCh
		// 	rf.commitIndex = index
		// 	// rf.lastApplied = rf.commitIndex
		// }
		rf.snapshot = snapshot
		rf.persist()
		if rf.isLeader && rf.offsetIndex == index+1 {
			// Check if there are peers lagging
			// A lagging peer would be one where nextIndex - 1(matchIndex possibly) is no longer in the log
			// Peers that are not lagging would have the snapshot, peers that are lagging won't have the snapshot.
			// We can update the peers here.
			for i, nextIndex := range rf.nextIndex {
				if i == rf.me {
					continue
				}
				if nextIndex <= index {
					go rf.relaySnapshots(i)
				}
			}
		}
	}

	// this function can kickstart the function checking peers

	/* Usually the snapshot will contain new information not already in the recipient’s log.
	   In this case, the follower discards its entire log; it is all superseded by the snapshot and may possibly have
	   uncommitted entries that conflict with the snapshot.

	   If instead the follower receives a snapshot that describes a prefix of its log (due to retransmission or by
	   mistake), then log entries covered by the snapshot are deleted but entries following the snapshot are still
	   valid and must be retained.
	*/

}

func (rf *Raft) relaySnapshots(server int) bool {
	count := 0
	for !rf.killed() {
		count++
		rf.mu.Lock()
		if !rf.isLeader {
			// If something changed that caused this snapshot to be irrelevant, then
			// exit
			DPrintf("[RAFT %v %v][RELAY SNAPSHOTS][SERVER %v] STOPPING RELAY\nisLeader: %v \n", rf.me, rf.currentTerm,
				server, rf.isLeader)
			rf.mu.Unlock()
			return false
		}
		if rf.nextIndex[server] > rf.lastIncludedIndex {
			rf.mu.Unlock()
			return true
		}
		DPrintf("[RAFT %v %v][RELAY SNAPSHOTS] sending InstallSnapshot RPC to server %v! lastIncludedIndex: %v\n",
			rf.me, rf.currentTerm, server, rf.lastIncludedIndex)
		args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot}
		rf.mu.Unlock()
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if ok {
			// rf.mu.Lock()
			// DPrintf("[RAFT %v %v][RELAY SNAPSHOTS][SERVER %v] successful RPC after %v tries\n",
			// 	rf.me, rf.currentTerm, server, count)
			// rf.mu.Unlock()
			return true
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
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
	// use applyCh to send the snapshot to the service using an ApplyMsg
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
	// already seen
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if len(rf.log) > args.LastIncludedIndex-rf.offsetIndex &&
		rf.log[args.LastIncludedIndex-rf.offsetIndex].Term == args.LastIncludedTerm {
		// keep everything up to this point
		rf.log = rf.log[args.LastIncludedIndex+1-rf.offsetIndex:]
	} else {
		// if the follower's log length is greater, empty our log
		rf.log = []LogEntry{}
	}
	DPrintf("[RAFT %v %v][INSTALL SNAPSHOT] successfully installed snapshot from leader %v!\n", rf.me, rf.currentTerm, args.LeaderId)
	DPrintf("[RAFT %v %v][INSTALL SNAPSHOT] updating offsetIndex: %v -> %v\n", rf.me, rf.currentTerm, rf.offsetIndex, args.LastIncludedIndex+1)
	DPrintf("commitIndex: %v -> %v | lastApplied: %v -> %v\n", rf.commitIndex, args.LastIncludedIndex, rf.lastApplied, args.LastIncludedIndex)
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

	// use applyCh here to apply snapshot to state machine

}

// This function will be called by a goroutine checking for peers that are lagging behind and can no longer be
// updated since the leader doesn't have the entries to update their logs
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return ok
	}
	if ok {
		if rf.currentTerm < reply.Term {
			DPrintf("[RAFT %v %v][SEND INSTALL SNAPSHOT] current term outdated! new term: %v\n", rf.me, rf.currentTerm,
				reply.Term)
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
		DPrintf("[RAFT %v %v][SEND INSTALL SNAPSHOT][SERVER %v] successful!\n", rf.me, rf.currentTerm, server)
	}

	return ok
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
		DPrintf("[Raft %v TERM %v] [REQUESTVOTE %v TERM %v] term not higher, no vote!\n", rf.me, rf.currentTerm,
			args.CandidateId, args.Term)
		return
	}
	// update our term if it's lower and reset who we voted for
	if rf.currentTerm < args.Term {
		DPrintf("[Raft %v TERM %v] [REQUESTVOTE %v TERM %v] higher term! Reset our vote!\n", rf.me, rf.currentTerm,
			args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// at least as up-to-date : compare index and term
		// if the requester has the larger latest term
		// OR they have the same latest term but the requester has a long greater than OR EQUAL
		// give vote
		DPrintf("[RAFT %v %v][REQUEST VOTE] haven't voted or already voted for the same candidate\n", rf.me, rf.currentTerm)

		// last log term is LITERALLY last log term
		// candidate's
		lastLogIndex := len(rf.log) + rf.offsetIndex - 1
		var lastLogTerm int
		if len(rf.log) == 0 {
			lastLogTerm = rf.lastIncludedTerm
		} else {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}

		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// grant vote
			DPrintf("[Raft %v %v] [REQUEST VOTE %v %v] granted vote!\n", rf.me, rf.currentTerm,
				args.CandidateId, args.Term)
			DPrintf("args: %v\n", args)
			DPrintf("log: %v\n", rf.log)
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
			// reset timeout only if we granted vote
			rf.electionTimeout = time.Now()
		} else {
			DPrintf("[RAFT %v %v][REQUEST VOTE %v %v] vote not granted!\nargs.LastLogTerm: %v | args.LastLogIndex: %v | lastLogTerm: %v | lastLogIndex: %v\n",
				rf.me, rf.currentTerm, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
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
	// if the reply term is greater, WE ARE NOT LEADER
	if args.Term != rf.currentTerm {
		return ok
	}
	if ok && reply.Term > rf.currentTerm {
		// response indicates we're not the latest
		// stop election for this server and update term
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

	// Below entries are used to improve log tracking efficiency for the leader
	XTerm  int
	XIndex int
	XLen   int
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
			DPrintf("[RAFT %v %v][APPLY ENTRIES] applying index %v (%v) to state machine\n", rf.me, rf.currentTerm,
				msg.CommandIndex-rf.offsetIndex, msg.CommandIndex)
			DPrintf("msg: %v\n\n", msg)
			rf.mu.Unlock()
			rf.applyCh <- msg
			// DPrintf("[RAFT %v %v][APPLY ENTRIES] new committed log: %v\n", rf.me, rf.currentTerm, rf.log[:endIndex])
		} else {
			rf.mu.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond)
		}

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("[RAFT] Entering append entries...\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] received call from server %v!\n", rf.me, rf.currentTerm, args.LeaderId, len(args.Entries), args.LeaderId)
	// If the term received is less than ours, then don't process (the sender is no longer the leader)
	if args.Term < rf.currentTerm {
		DPrintf("[FOLLOWER %v][APPEND ENTRIES] args term not greater! me: %v | args %v\n", rf.me, rf.currentTerm, args.Term)
		return
	} else {
		// DPrintf("[Raft %v] [APPEND ENTRIES] term condition met!\n", rf.me)
		// Reset electionTimeout
		rf.electionTimeout = time.Now()
		// DPrintf("[RAFT %v %v][APPEND ENTRIES] election time reset: %v\n", rf.me, rf.currentTerm, rf.electionTimeout)

		// Update our term if we find a higher term or same term (this should stop election)
		if args.Term > rf.currentTerm {
			DPrintf("[RAFT %v %v][APPEND ENTRIES] higher term found! %v -> %v\n", rf.me, rf.currentTerm,
				rf.currentTerm, args.Term)
			rf.currentTerm = args.Term
			rf.isLeader = false
			rf.votedFor = -1
			rf.persist()
		}
		// Reply false if log does not have an entry at prevLogIndex whose term matches prevLogTerm
		logLength := rf.offsetIndex + len(rf.log)
		DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] args.LeaderCommit: %v | args.PrevLogIndex %v | len(rf.log) %v | args.PrevLogTerm %v\n rf.lastIncludedIndex: %v | rf.lastIncludedTerm: %v\n",
			rf.me, rf.currentTerm, args.LeaderId, len(args.Entries), args.LeaderCommit, args.PrevLogIndex, logLength, args.PrevLogTerm, rf.lastIncludedIndex, rf.lastIncludedTerm)
		DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] offsetIndex: %v\n", rf.me, rf.currentTerm, args.LeaderId, len(args.Entries), rf.offsetIndex)
		if args.PrevLogIndex != rf.lastIncludedIndex && args.PrevLogIndex < rf.offsetIndex {
			return
		}
		if logLength <= args.PrevLogIndex ||
			(args.PrevLogIndex == rf.lastIncludedIndex && rf.lastIncludedTerm != args.PrevLogTerm) ||
			(args.PrevLogIndex != rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.offsetIndex].Term != args.PrevLogTerm) {
			// DPrintf("[FOLLOWER %v][APPEND ENTRIES] second check failed! len(rf.log) %v | args.PrevLogIndex: %v | this prev term %v | args prev term %v\n", rf.me, len(rf.log), args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

			// Find values for reply.X*
			// the follower can include the term of the conflicting entry and the first index it stores for that term
			reply.XLen = logLength
			reply.XIndex = args.PrevLogIndex

			if logLength <= args.PrevLogIndex {
				// log doesn't have prevLogIndex at all
				DPrintf("[RAFT %v %v][APPEND ENTRIES] loglength: %v | args.PrevLogIndex: %v\n", rf.me, rf.currentTerm,
					len(rf.log)+rf.offsetIndex, args.PrevLogIndex)
				return
			} else if args.PrevLogIndex == rf.lastIncludedIndex || rf.log[args.PrevLogIndex-rf.offsetIndex].Term == rf.lastIncludedTerm {
				reply.XTerm = rf.lastIncludedTerm
				reply.XIndex = rf.lastIncludedIndex
				return
			} else {
				reply.XTerm = rf.log[args.PrevLogIndex-rf.offsetIndex].Term
				// the term at prevLogIndex doesn't match the term from the leader
				// first try the term that we do have
				// second option is to get the index of the first entry with the matching term from the leader
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
			DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] prev check successful!\n", rf.me, rf.currentTerm, args.LeaderId, len(args.Entries))
			reply.Success = true
			if len(args.Entries) > 0 {
				DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] appending entries to log!\n", rf.me, rf.currentTerm, args.LeaderId, len(args.Entries))
				for i, entry := range args.Entries {
					thisIndex := args.PrevLogIndex + 1 + i - rf.offsetIndex
					if thisIndex < len(rf.log) && rf.log[thisIndex].Term == entry.Term {
						continue
					} else {
						rf.log = rf.log[:thisIndex]
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}
				}
				rf.persist()
				DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] log: %v\n", rf.me, rf.currentTerm, args.LeaderId, len(args.Entries), rf.log)
			}
		}
		// Update follower commit index to be min(LeaderCommit, last index of updated rf.log)
		if args.LeaderCommit > rf.commitIndex {
			var newCommit int = args.LeaderCommit
			if newCommit > args.PrevLogIndex+len(args.Entries) {
				newCommit = args.PrevLogIndex + len(args.Entries)
			}
			// DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] updated commitIndex: %v -> %v | args.LeaderCommit: %v | loglength: %v\nlog: %v", rf.me, rf.currentTerm,
			// 	args.LeaderId, len(args.Entries), rf.commitIndex, newCommit, args.LeaderCommit, len(rf.log), rf.log)
			// DPrintf("[RAFT %v %v][APPEND ENTRIES %v %v] args.PrevLogIndex: %v | args.PrevLogTerm: %v\n", rf.me,
			// 	rf.currentTerm, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)
			// if newCommit < rf.commitIndex {
			// 	log.Fatalf("[RAFT %v %v][APPEND ENTRIES %v %v] newCommit < rf.commitIndex | %v < %v | args.LeaderCommit: %v | len(args.Entries): %v | args.PrevLogIndex: %v | rf.offsetIndex: %v\n",
			// 		rf.me, rf.currentTerm, args.LeaderId, len(args.Entries), newCommit, rf.commitIndex, args.LeaderCommit, len(args.Entries), args.PrevLogIndex, rf.offsetIndex)
			// }
			if newCommit > rf.commitIndex {
				rf.commitIndex = newCommit
			}
		}
		// DPrintf("[RAFT %v %v] [APPEND ENTRIES] done!\n", rf.me, rf.currentTerm)
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Update term whenever we can
	// Tells the server that we are not the leader (no more heartbeats)

	if args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		DPrintf("[RAFT %v %v][SEND APPEND ENTRIES] current term outdated! new term: %v\n", rf.me, rf.currentTerm,
			reply.Term)
		rf.currentTerm = reply.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	// if the check is unsuccessful, we decrement nextIndex
	// if it's for the term not matching, it means that the entries are not matching
	if ok {
		if !reply.Success && rf.nextIndex[server] == args.PrevLogIndex+1 {
			if reply.XLen <= args.PrevLogIndex {
				// If the length is shorter than our log, we should use different length
				DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v] using XLen %v\n", rf.me, rf.currentTerm, server, reply.XLen)
				rf.nextIndex[server] = reply.XLen
			} else {
				// check if leader has XTerm
				// we are leader
				// the length of the peer's log is as long or longer than ours
				// BUT, the term doesn't match at the index
				// so we have the peer's term, and the index, so we should start at the prevLogIndex for looking in
				// our own log.
				// MatchingTermIndex := the latest index in the leader's log with a matching term
				// XIndex := the earliest index that has the corresponding term
				matchingTermIndex := -1
				DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v]log length: %v | rf.nextIndex[%v]: %v\n", rf.me,
					rf.currentTerm, server, len(rf.log)+rf.offsetIndex, server, rf.nextIndex[server])
				DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v] reply: %v\n", rf.me, rf.currentTerm, server, reply)
				for i := len(rf.log) - 1; i >= 0; i-- {
					if rf.log[i].Term < reply.XTerm {
						// log has entries stored in term order (non-descending)
						break
					}
					if rf.log[i].Term == reply.XTerm {
						matchingTermIndex = i + rf.offsetIndex
						break
					}
				}
				if matchingTermIndex == -1 {
					// leader's log has no entry that matches the term
					DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v] using XIndex for nextIndex: %v\n", rf.me, rf.currentTerm, server, reply.XIndex)
					rf.nextIndex[server] = reply.XIndex
				} else {
					DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v] using first match index for nextIndex: %v\n", rf.me, rf.currentTerm, server, matchingTermIndex)
					rf.nextIndex[server] = matchingTermIndex
				}
			}

		} else if reply.Success {
			// update nextIndex and matchIndex for non-heartbeat sends
			if len(args.Entries) > 0 && args.PrevLogIndex+1+len(args.Entries) > rf.nextIndex[server] {
				DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v] updating nextIndex: %v -> %v\n", rf.me, rf.currentTerm,
					server, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries))
				rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				DPrintf("[RAFT %v %v][SEND APPEND ENTRIES][SERVER %v] args.PrevLogIndex: %v | len(args.Entries): %v | nextIndex: %v\n",
					rf.me, rf.currentTerm, server, args.PrevLogIndex, len(args.Entries), rf.nextIndex[server])
			}
		}
	}

	return ok
}

func (rf *Raft) updateCommitIndex() {
	// This go routine checks for logs to be committed to the state machine as the leader. The leader needs to commit
	// when it finds a commitIndex higher than the current index for the current term where a majority of the other raft
	// peers have the same entry for the same term
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		indexCounts := map[int]int{}
		majorityIndex := rf.commitIndex
		minMajority := len(rf.peers)/2 + 1
		for _, mIndex := range rf.matchIndex {
			// DPrintf("[RAFT %v %v][UPDATE COMMIT INDEX] mIndex: %v | len log + offset: %v | offset: %v\n", rf.me, rf.currentTerm, mIndex, len(rf.log)+rf.offsetIndex, rf.offsetIndex)
			for mIndex >= rf.commitIndex && ((mIndex > rf.lastIncludedIndex && rf.log[mIndex-rf.offsetIndex].Term == rf.currentTerm) ||
				(mIndex == rf.lastIncludedIndex && rf.lastIncludedTerm == rf.currentTerm)) {
				indexCounts[mIndex]++
				mIndex--
			}
		}
		for index, count := range indexCounts {
			if count >= minMajority && index > majorityIndex {
				majorityIndex = index
			}
		}

		if majorityIndex == -1 || rf.commitIndex == majorityIndex {
			// don't do anything if we couldn't find a newer majority index or the commitIndex is the same as our current one
			rf.mu.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond)
			continue
		} else {
			DPrintf("[RAFT %v %v][COMMIT COMMANDS] majorityIndex %v found! old: %v\n", rf.me, rf.currentTerm,
				majorityIndex, rf.commitIndex)
			// MajorityIndex found! Apply changes to state machine
			DPrintf("[RAFT %v %v][COMMIT COMMANDS] LEADER updating commits from %v to %v\n", rf.me, rf.currentTerm, rf.commitIndex, majorityIndex)
			rf.commitIndex = majorityIndex
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

}

func (rf *Raft) logAgreement(peers int, term int) {
	// This go routine is started when the leader is elected. It performs the
	// RPCs for adding and updating follower log entries. There is a separate
	// go routine that will check for commits
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		lastLogIndex := len(rf.log) + rf.offsetIndex - 1
		// Check with all of the peers if there are any logs that need to be sent out. The leader will periodically
		// check this for each of the peers, and for each peer, the log will be updated until it's in sync.
		// The check for each peer shouldn't have repeat checks, so we'll use a bool array to make sure this check isn't
		// being repetitive.

		for i, nextIndex := range rf.nextIndex {
			if i == rf.me {
				continue
			}

			/* Skip this peer if:
			1. It's already being checked
			2. The last log index in the leader's log is less than the nextIndex
			3. The log is empty
			4. The nextIndex that the peer is expecting starts at an index less than rf.offsetIndex
			*/
			if lastLogIndex < nextIndex {
				continue
			}

			// set up args here?
			go func(server int, term int) {
				rf.mu.Lock()
				if !rf.isLeader || rf.currentTerm != term {
					// DPrintf("[RAFT %v %v][LOG AGREEMENT] [server %v] no longer leader! stop sending! leader: %v | term: %v\n",
					// 	rf.me, rf.currentTerm, server, rf.isLeader, term)
					rf.mu.Unlock()
					return
				}
				nextIndex := rf.nextIndex[server]
				DPrintf("[RAFT %v %v][LOG AGREEMENT][SERVER %v] nextIndex: %v | offsetIndex: %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.offsetIndex)
				if nextIndex < rf.offsetIndex {
					offsetIndex := rf.offsetIndex
					DPrintf("[RAFT %v %v][LOG AGREEMENT][SERVER %v] need to send snapshot! offsetIndex: %v\n",
						rf.me, rf.currentTerm, server, rf.offsetIndex)
					rf.mu.Unlock()

					if !rf.relaySnapshots(server) {
						return
					} else {
						rf.mu.Lock()
						DPrintf("[RAFT %v %v][LOG AGREEMENT][SERVER %v] established snapshot! offsetIndex: %v\n",
							rf.me, rf.currentTerm, server, offsetIndex)
						rf.mu.Unlock()
						return
					}
				}
				lastLogIndex := len(rf.log) + rf.offsetIndex - 1
				if len(rf.log) == 0 || lastLogIndex < nextIndex {
					rf.mu.Unlock()
					return
				}

				// If the last log index is greater than or equal to a follower, then we want to send log entries
				// starting at nextIndex
				//	- if successful, update nextIndex and matchIndex
				//	- if failed due to log inconsistency, decrement nextIndex and retry
				// nextIndex stores next log entry to send, so prevLogIndex is the last log entry before the new
				// ones.

				prevLogIndex := nextIndex - 1
				prevLogTerm := rf.lastIncludedTerm
				if prevLogIndex != rf.lastIncludedIndex {
					prevLogTerm = rf.log[prevLogIndex-rf.offsetIndex].Term
				}

				args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, []LogEntry{}, rf.commitIndex}
				reply := AppendEntriesReply{}
				// Add the new log entries that start from rf.nextIndex[server] and onwards

				// If nextIndex is less than offsetIndex, then we can't update and must use a snapshot to update
				// but we can't wait for snapshot to be called:
				// 	1. There could be a follower that was exceptionally slow and did no
				args.Entries = append(args.Entries, rf.log[nextIndex-rf.offsetIndex:]...)

				if len(args.Entries) <= 0 {
					log.Fatal("[LOG AGREEMENT][ERROR] entries must have at least one element!\n")
				}
				// DPrintf("[LOG ENTRIES SIZE] %v\n", len(args.Entries))
				// DPrintf("[RAFT %v %v][LOG AGREEMENT][server %v] RETRYING LOG AGREEMENT\n", rf.me, rf.currentTerm, server)
				rf.mu.Unlock()
				for !rf.killed() {
					rf.mu.Lock()
					if !rf.isLeader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok && reply.Success {
						DPrintf("[LOG AGREEMENT] successful for server %v\n", server)

						// only want to stop if leader was successful at reaching the peer
						return
					}
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
			}(i, term)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(25) * time.Millisecond)
	}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + rf.offsetIndex
	term := rf.currentTerm
	isLeader := rf.isLeader
	if !isLeader {
		return index, term, isLeader
	} else {
		// is leader -> append command to leader's log
		DPrintf("[RAFT %v %v] [START] appending log to leader's log at index %v\n",
			rf.me, rf.currentTerm, len(rf.log)+rf.offsetIndex)
		DPrintf("current log: %v\n", rf.log)
		DPrintf("command: %v\n\n", command)
		rf.log = append(rf.log, LogEntry{command, term})
		rf.persist()
		rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.offsetIndex
		// return immediately
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Ensures that the leader and other followers do not start elections.
func (rf *Raft) heartbeat(peers int, term int) {
	// This go routine sends the heartbeat messages to the followers
	for !rf.killed() {
		// Check if we're still the leader
		rf.mu.Lock()
		if !rf.isLeader || rf.currentTerm != term {
			DPrintf("[Raft %v] [HEARTBEAT] lost leader status or changed terms| isLeader: %v | term: %v -> %v\n", rf.me, rf.isLeader, term, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		// Send heartbeats to each of the followers (except ourselves)
		for i := 0; i < peers; i++ {
			if i == rf.me {
				continue
			}

			go func(server int, term int) {
				// Repeatedly send RPC until we reach follower
				for !rf.killed() {
					rf.mu.Lock()
					if !rf.isLeader || rf.currentTerm != term {
						rf.mu.Unlock()
						return
					}
					nextIndex := rf.nextIndex[server]
					prevLogIndex := nextIndex - 1
					prevLogTerm := rf.lastIncludedTerm
					if prevLogIndex > rf.lastIncludedIndex {
						prevLogTerm = rf.log[prevLogIndex-rf.offsetIndex].Term
					} else if prevLogIndex < rf.lastIncludedIndex {
						DPrintf("[RAFT %v %v][HEARTBEAT][SERVER %v] prevLogIndex < lastIncludedIndex ! %v < %v -> SNAPSHOT NEEDED!\n",
							rf.me, rf.currentTerm, server, prevLogIndex, rf.lastIncludedIndex)
						DPrintf("[RAFT %v %v][HEARTBEAT][SERVER %v] need to send snapshot! offsetIndex: %v\n",
							rf.me, rf.currentTerm, server, rf.offsetIndex)
						rf.mu.Unlock()

						if !rf.relaySnapshots(server) {
							return
						} else {
							rf.mu.Lock()
							DPrintf("[RAFT %v %v][HEARTBEAT][SERVER %v] established snapshot!\n", rf.me, rf.currentTerm,
								server)
							rf.mu.Unlock()
							return
						}
					}

					args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, []LogEntry{},
						rf.commitIndex}
					// DPrintf("[RAFT %v][HEARTBEAT][SERVER %v] sending heartbeat!\n", rf.me, server)
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok {
						// rf.mu.Lock()
						// DPrintf("[RAFT %v %v][HEARBEAT][SERVER %v] responded to heartbeat!\n", rf.me, rf.currentTerm, server)
						// rf.mu.Unlock()
						return
					}
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
			}(i, term)
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(101) * time.Millisecond)
	}
}

func (rf *Raft) election(startTime time.Time, term int) {
	// Elect for myself
	// increment
	rf.mu.Lock()
	peers := len(rf.peers)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.log) == 0 {
		args.LastLogIndex = rf.lastIncludedIndex
		args.LastLogTerm = rf.lastIncludedTerm
	} else {
		args.LastLogIndex = len(rf.log) - 1 + rf.offsetIndex
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	// rf.votedFor = rf.me
	rf.persist()

	count := 1
	voteMu := sync.Mutex{}
	voteCond := sync.NewCond(&voteMu)
	finished := 1
	var majority int = len(rf.peers)/2 + 1

	// Send the message to all the peers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// Check if we're still the candidate or if a new election started or if a leader has been selected already
		// 1. The term could have changed
		// 2. The electionTimeout could have changed meaning this election took too long
		if rf.currentTerm != term || rf.electionTimeout.Sub(startTime).Milliseconds() > 0 {
			// end election and get out if we're no longer a candidate
			// or we started a new election
			DPrintf("[RAFT %v %v][ELECTION %v] no longer candidate or new election %v started!\n", rf.me,
				rf.currentTerm, term, rf.currentTerm)
			DPrintf("start time %v | election time %v\n", startTime, rf.electionTimeout)
			rf.mu.Unlock()
			return
		}
		DPrintf("[RAFT %v %v][ELECTION %v] sending RequestVote to server %v!\n", rf.me, rf.currentTerm, term, i)
		// send votes for all peers
		go func(server int, candidateArgs RequestVoteArgs) {
			var reply RequestVoteReply
			// retry indefinitely
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
				time.Sleep(time.Duration(10) * time.Millisecond)
				// may need to sleep or use condition variables here
			}
			voteMu.Lock()
			defer voteMu.Unlock()
			// if ok, it means we're still a candidate
			if reply.VoteGranted {
				count++
			}
			// DPrintf("[RAFT %v %v][ELECTION %v] vote received from server %v!\n", rf.me, rf.currentTerm, term, server)
			finished++
			voteCond.Broadcast()
		}(i, args)
	}

	// Only reaches here if election is still the same
	voteMu.Lock()
	for count < majority && finished != peers {
		// Need to return if election lost or took too long
		if rf.currentTerm != term || rf.electionTimeout.Sub(startTime).Milliseconds() > 0 {
			rf.mu.Unlock()
			voteMu.Unlock()
			return
		}
		DPrintf("[RAFT %v %v][ELECTION %v] waiting on condition -> count: %v, majority: %v | finished: %v, peers: %v \n",
			rf.me, rf.currentTerm, term, count, majority, finished, peers)
		rf.mu.Unlock()
		voteCond.Wait()
		rf.mu.Lock()
	}
	DPrintf("[RAFT %v %v][ELECTION %v] outside loop | count/majority: %v/%v | finished/peers: %v/%v\n",
		rf.me, rf.currentTerm, term, count, majority, finished, peers)
	// If we have a quorum, then we can be selected as leader!
	if count >= majority {
		if rf.currentTerm != term || rf.electionTimeout.Sub(startTime).Milliseconds() > 0 {
			DPrintf("[RAFT %v %v][ELECTION %v] term changed, invalid election: %v -> %v\n", rf.me, rf.currentTerm,
				term, term, rf.currentTerm)
		} else {
			// won election
			// start heartbeat process
			DPrintf("[RAFT %v %v][ELECTION %v] WON ELECTION, received %v/%v majority votes!\n", rf.me, rf.currentTerm,
				term, count, peers)
			DPrintf("\n------------------[LEADER ANOUNCEMENT] %v IS THE LEADER !!!!!!!!!!!!!!!!!![LEADER ANOUNCEMENT]----------------\n\n", rf.me)
			// reinitialized after election for leaders only
			rf.nextIndex = make([]int, peers)
			rf.matchIndex = make([]int, peers)
			for i := range rf.nextIndex {
				// initialized to leader's last log index + 1 (length of the log)
				rf.nextIndex[i] = len(rf.log) + rf.offsetIndex
			}
			rf.isLeader = true
			// Start heartbeat and communicating log additions to peers (leader specific)
			go rf.heartbeat(peers, rf.currentTerm)
			go rf.logAgreement(peers, rf.currentTerm)
			go rf.updateCommitIndex()
		}
	} else {
		DPrintf("[RAFT %v %v][ELECTION] majority not reached, lost election. %v of %v\n", rf.me, rf.currentTerm, count, peers)
	}
	rf.mu.Unlock()
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
		ms := 500 + (rand.Int63() % 800)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		//
		// During this time, the election timeout can be altered by
		// AppendEntries
		// RequestVote
		//
		rf.mu.Lock()
		// DPrintf("[RAFT %v %v][TICKER] waking up after sleeping for %v ms\n", rf.me, rf.currentTerm, ms)
		// After sleeping, if the electionTimeout value is more than ms away it means it was not altered.
		// If we're the leader, we won't need to check electionTimeout
		if !rf.isLeader && time.Since(rf.electionTimeout).Milliseconds() >= ms {
			DPrintf("[RAFT %v %v][TICKER] time since: %v / %v\n", rf.me, rf.currentTerm, time.Since(rf.electionTimeout).Milliseconds(),
				ms)
			// reset election timer before the start of a new election
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()
			rf.electionTimeout = time.Now()
			DPrintf("[RAFT %v %v][TICKER] starting election...%v | electionTimeout: %v\n", rf.me, rf.currentTerm, rf.currentTerm, rf.electionTimeout)
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

	DPrintf("[Raft %v] starting up...\n", me)
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
	rf.applyCh = applyCh

	// 2D
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.offsetIndex = 0
	rf.snapshot = nil
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// rf.persist()
	// start ticker goroutine to start elections
	// ticker -> election -> heartbeats
	go rf.ticker()
	go rf.applyEntries()

	return rf
}
