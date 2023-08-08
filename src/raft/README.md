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
