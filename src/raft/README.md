# Raft
Note: the original template of this codebase is borrowed from [MIT's 6.5840 Distributed Systems course](https://pdos.csail.mit.edu/6.824/).

The main implementation of the Raft consensus protocol is located in `raft.go`, with tests provided by `test_test.go`, which utilizes a modified RPC that can simulate delays, unreliable connections, and disconnects to test the protocol under conditions it needs to be fault tolerant to. 

Each Raft peer has the following state: 
```golang
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

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
```

Each Raft has the following RPCs to accomplish the distributed consensus' goals:
* InstallSnapshot
* RequestVote
* AppendEntries

For each Raft peer, they have the following functions to maintain the peer:
* persist
* readPersist
* Snapshot
* relaySnapshots
* sendInstallSnapshot
* sendRequestVote
* applyEntries
* sendAppendEntries
* updateCommitIndex
* logAgreement
* heartbeat
* election
* ticker
* Start
* killed

There are some other functions used for the tester such as `Kill()` and `GetState()`.


## RPCs
The RPCs were designed following the guidelines provided by the original Raft paper's Figure 2, where the various types of servers (Followers, Candidates, Leaders) have their specifications listed. 
![Raft Figure 2](raft_figure2.png)

