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
	"github.com/cmu440/labrpc"
	"math/rand"
	"sync"
	"time"
	"fmt"
)

const MAX = 1024

const (
	LEADER = iota
	CANDIDATE
	FLLOWER
)

const (
	ELECTIME = 350 * time.Millisecond
	HBTIME   = 100 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	state     int
	voteCount int

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	grantVoteCh  chan bool
	appendEntrCh chan bool
	allVoteCh    chan bool
	commitCh     chan bool
	applyCh      chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FLLOWER
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index {
		rf.state = FLLOWER
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- true
		reply.VoteGranted = true
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term) {
		rf.state = FLLOWER
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- true
		reply.VoteGranted = true
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || args.PrevLogIndex < 0 || args.PrevLogIndex > rf.log[len(rf.log)-1].Index || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FLLOWER
		rf.votedFor = -1
	}
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = rf.log[len(rf.log)-1].Index
		if args.LeaderCommit < rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitCh <- true
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.appendEntrCh <- true
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
		} else if reply.VoteGranted {
			rf.voteCount++
			majority := len(rf.peers) / 2
			if rf.state == CANDIDATE && rf.voteCount > majority {
				rf.allVoteCh <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FLLOWER
			rf.votedFor = -1
		} else if reply.Success {
			rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1:])
			}
			var reply AppendEntriesReply
			go rf.sendAppendEntries(server, args, &reply)
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state == LEADER {
		isLeader = true
		index = rf.log[len(rf.log)-1].Index + 1
		log := LogEntry{
			Term:    term,
			Command: command,
			Index:   index,
		}
		rf.log = append(rf.log, log)
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = FLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.appendEntrCh = make(chan bool, MAX)
	rf.grantVoteCh = make(chan bool, MAX)
	rf.allVoteCh = make(chan bool, MAX)
	rf.commitCh = make(chan bool, MAX)
	rf.applyCh = applyCh
	go rf.runRaft()
	go rf.applyMessage()
	return rf
}

func (rf *Raft) runRaft() {
	for {
		rf.mu.Lock()
		switch rf.state {
		case FLLOWER:
			rf.mu.Unlock()
			ticker := time.After(time.Duration(rand.Int63n(100))*time.Millisecond + ELECTIME)
			select {
			case <-ticker:
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
			case <-rf.grantVoteCh:
			case <-rf.appendEntrCh:
			}
		case CANDIDATE:
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			ticker := time.After(time.Duration(rand.Int63n(100))*time.Millisecond + ELECTIME)
			//sends out Request Vote to all rafts
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
			}
			rf.mu.Unlock()
			for server := range rf.peers {
				if server != rf.me {
					var reply RequestVoteReply
					go rf.sendRequestVote(server, args, &reply)
				}
			}
			select {
			case <-ticker:
			case <-rf.appendEntrCh:
				rf.mu.Lock()
				rf.state = FLLOWER
				rf.votedFor = -1
				rf.mu.Unlock()
			case <-rf.allVoteCh:
				rf.mu.Lock()
				rf.state = LEADER
				for i := range rf.peers {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			}
		case LEADER:
			for i := rf.commitIndex + 1; i <= rf.log[len(rf.log)-1].Index; i++ {
				majority := len(rf.peers) / 2
				for j := range rf.peers {
					if rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
						majority--
						if majority <= 0 {
							rf.commitIndex = i
							rf.commitCh <- true
							break
						}
					}
				}
			}
			for server := range rf.peers {
				if server != rf.me {
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: rf.nextIndex[server] - 1,
					}
					if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
						copy(args.Entries, rf.log[args.PrevLogIndex+1:])
					}
					//fmt.Println(args.PrevLogIndex)
					//fmt.Println(rf.log[args.PrevLogIndex].Command)
					var reply AppendEntriesReply
					go rf.sendAppendEntries(server, args, &reply)
				}
			}
			rf.mu.Unlock()
			time.Sleep(HBTIME)
		}
	}
}

func (rf *Raft) applyMessage() {
	for {
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				args := ApplyMsg{
					Index:   i,
					Command: rf.log[i].Command,
				}
				//fmt.Println(rf.log[i].Command)
				rf.applyCh <- args
				fmt.Println("leader ", rf.me, "committed command ", args.Command, "at term:", rf.currentTerm, "index:", args.Index)
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
		}
	}
}
