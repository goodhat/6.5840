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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	// 1: follower, 2: candidate, 3: leader
	STATE_FOLLOWER  = 1
	STATE_CANDIDATE = 2
	STATE_LEADER    = 3

	LEADER_FLUSH_INTERVAL      = 100
	RPC_APPEND_ENTRIES_TIMEOUT = 50
	RPC_REQUEST_VOTE_TIMEOUT   = 30

	TICKER_INTERVAL_MIN = 50
	TICKER_INTERVAL_MAX = 300
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
	LogIndex int
	Term     int
	Command  interface{}
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
	applyCh      chan ApplyMsg
	state        int // 1: follower, 2: candidate, 3: leader
	currentTerm  int
	votedFor     int
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	lastLogIndex int
	lastLogTerm  int

	nextIndex  []int
	matchIndex []int

	lastHeartBeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == STATE_LEADER
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
	Term         int
	VotedGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Debug(dElection, "RequestVote from %d with term %d", args.CandidateId, args.Term)
	defer func() {
		rf.Debug(dElection, fmt.Sprintf("Vote for %d with term %d: %t", args.CandidateId, args.Term, reply.VotedGranted))
	}()

	// The election is too old. Reject it and provide current term.
	if rf.currentTerm > args.Term {
		reply.VotedGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// The election has the same term.
	if rf.currentTerm == args.Term {
		// This should probably be a very rare case.
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && // Haven't voted yet, or already voted for the same candidate
			args.LastLogIndex >= rf.lastLogIndex { // The candidate's log is longer.
			reply.VotedGranted = true
			reply.Term = rf.currentTerm

			// Critical section
			rf.votedFor = args.CandidateId
			rf.lastHeartBeat = time.Now()
		} else {
			// If the term is the same, usually means that this node already vote for someone and update the term.
			reply.VotedGranted = false
			reply.Term = rf.currentTerm
		}
		return
	}

	if rf.currentTerm < args.Term {
		reply.VotedGranted = true
		reply.Term = rf.currentTerm

		// Critical section
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.lastHeartBeat = time.Now()
		if rf.state == STATE_LEADER {
			rf.state = STATE_FOLLOWER
		}
		return
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
func (rf *Raft) sendRequestVote(ctx context.Context, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ch := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()

	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		return false
	}
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PreLogTerm        int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// You are too old. Humble it!
	rf.Debug(dInfo, fmt.Sprintf("Get append entries from S%d", args.LeaderId))
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.refreshLastHeartBeat()

	// Oh wow. I am too old. I need to refresh my term.
	if rf.currentTerm < args.Term {
		if rf.state == STATE_LEADER {
			rf.state = STATE_FOLLOWER
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.votedFor = -1
		return
	}

	// Default
	reply.Term = rf.currentTerm
	reply.Success = true
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
	// Early return without locking.
	if rf.state != STATE_LEADER {
		return len(rf.log), rf.currentTerm, false
	}

	// Critical section
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != STATE_LEADER {
		return len(rf.log), rf.currentTerm, false
	}
	index := len(rf.log) + 1
	term := rf.currentTerm

	// Your code here (2B).
	entry := LogEntry{
		LogIndex: index,
		Command:  command,
		Term:     term,
	}
	rf.log = append(rf.log, entry)

	return index, term, true
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

func (rf *Raft) flushLog() {
	if rf.state != STATE_LEADER {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ch := make(chan bool, len(rf.peers))
	// cancelCh := make(chan bool, len(rf.peers))
	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	// var wg sync.WaitGroup
	// defer func() {
	// 	// wg.Wait()
	// 	// cancel()
	// 	close(ch)
	// 	close(cancelCh)
	// }()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// wg.Add(1)
		// go func(i int, ch chan )
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// PrevLogIndex:      rf.nextIndex[i] - 1,
			// PreLogTerm:        rf.log[rf.nextIndex[i]-1].Term,
			// Entries:           rf.log[rf.nextIndex[i]-1:],
			// LeaderCommitIndex: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		go rf.sendAppendEntries(i, args, reply)
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		if rf.state != STATE_LEADER {
			continue
		}
		go rf.flushLog()
		time.Sleep(time.Duration(LEADER_FLUSH_INTERVAL) * time.Millisecond)
	}
}

func (rf *Raft) raiseElection() {
	rf.Debug(dElection, "Become a candidate and start an election.")

	// Critical seciton
	rf.mu.Lock()
	rf.state = STATE_CANDIDATE
	rf.lastHeartBeat = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	electionTerm := rf.currentTerm // Need to record the term of this election.
	rf.mu.Unlock()

	voteCh := make(chan bool, len(rf.peers))
	humbleCh := make(chan bool, len(rf.peers)) // When receving larger term, use this channel to exit quickly.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		cancel() // TODO: should be at wait() front?
		close(voteCh)
		close(humbleCh)
	}()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastLogIndex,
				LastLogTerm:  rf.lastLogTerm,
			}
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(ctx, i, args, reply)
			if ok {
				// Get a fresher term. Need to catch up.
				if reply.Term > rf.currentTerm {
					// Critical section
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = STATE_FOLLOWER
						humbleCh <- true
						// cancel() // cancel to make other RPC calls return earlier .
						rf.Debug(dElection, "Get vote from %d, term=%d %t but cancel", i, reply.Term, reply.VotedGranted)
					}
					rf.mu.Unlock()
				} else {
					rf.Debug(dElection, "Get vote from %d, term=%d %t", i, reply.Term, reply.VotedGranted)
					voteCh <- reply.VotedGranted
				}
			} else {
				rf.Debug(dElection, "Get vote from %d, ok=false", i)
				voteCh <- false
			}
		}(i)
	}

	receivedVotes := 0
	allVotes := 0
	for {
		select {
		case vote := <-voteCh:
			allVotes++
			if vote {
				receivedVotes++
			}

			// If it gets a majority of votes, and the currentTerm is still the same, become a leader!
			// If the currentTerm is not the same, it means that it is updated by some RPC calls, and the node is probably not a candidate anymore.
			if (receivedVotes+1)*2 >= len(rf.peers) && electionTerm == rf.currentTerm {
				// Critical seciton (need double checking)
				rf.mu.Lock()
				if rf.state == STATE_CANDIDATE && electionTerm == rf.currentTerm {
					rf.Debug(dLeader, "========= Become a leader =========")
					rf.state = STATE_LEADER
				}
				rf.mu.Unlock()
				return
			}

			// Already get all votes.
			if allVotes == len(rf.peers)-1 {
				return
			}

		case <-humbleCh:
			return
		}
	}

}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.state == STATE_LEADER {
			continue
		}

		// Check if there's a heartbeat happened during the interval.
		if time.Since(rf.lastHeartBeat) <= time.Duration(ms)*time.Millisecond {
			rf.Debug(dTimer, "Good heartbeat")
			continue
		}

		// No heartbeat since last heart beat.
		// Become a candidate
		go rf.raiseElection()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastHeartBeat = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderTicker()

	return rf
}
