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
    "math/rand"
    "sync"
    "sync/atomic"
    "time"
    "fmt"

    "6.5840/labgob"
    "6.5840/labrpc"
)

const DEBUG = false

func (this *Raft) debug(format string, args ...interface{}) {
	if DEBUG {
        var leader string
        if (this.votedFor == this.me) {
            leader = "*"
        } else {
            leader = ""
        }
        prefix := fmt.Sprintf("%v[server_%v_term_%v] (alive: %v) ",
                              leader, this.me, this.currentTerm, !this.killed())
		fmt.Printf(prefix + format, args...)
	}
}

const (
    /* retry cnt when VOTE rpc failed caused by network issues */
    RPC_RETRY_VOTE              = 1
    /* retry cnt when APPEND rpc failed caused by network issues */
    RPC_RETRY_APPEND            = 5
    /* retry cnt when SNAPSHOT rpc failed caused by network issues */
    RPC_RETRY_SNAPSHOT          = 2
    /* rpc timeout */
    RPC_TIMEOUT_MS              = 30
    /* leader send heartbeat rpc every HEARTBEAT_TICK_MS ms */
    HEARTBEAT_TICK_MS           = 50
    /*
     * FOLLOWER timeout if it has not got heartbeat rpc in
     * HEARTBEAT_TIMEOUT_MS_BASE + rand(HEARTBEAT_TIMEOUT_MS_DELTA) ms
     */
    //HEARTBEAT_TIMEOUT_MS_BASE   = 150
    HEARTBEAT_TIMEOUT_MS_BASE   = 200
    HEARTBEAT_TIMEOUT_MS_DELTA  = 300
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid        bool
	Command             interface{}
	CommandIndex        int

	SnapshotValid       bool
	Snapshot            []byte
	SnapshotTerm        int
	SnapshotIndex       int
}

type Entry struct {
    Term                int
    Index               int
    Val                 interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                  sync.Mutex          // Lock to protect shared access to this peer's state
	peers               []*labrpc.ClientEnd // RPC end points of all peers
	persister           *Persister          // Object to hold this peer's persisted state
	me                  int                 // this peer's index into peers[]
	dead                int32               // set by Kill()

    /* protocol properties defined in https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf */
    currentTerm         int
    votedFor            int
    log                 []Entry
    commitIndex         int             /* index of highest log entry known to be committed */
    lastApplied         int             /* index of highest log entry applied to state machine */
    nextIndex           map[int]int     /* index of the next log entry to send to each server */
    matchIndex          map[int]int     /* index of highest log entry known to be replicated to each server */

    /* implementing properties */
    applyCh             chan ApplyMsg
    broadcaster         Broadcaster
    snapshot            []byte
    snapshotTerm        int
    snapshotIndex       int
}

type RequestVoteArgs struct {
    Term                int
    CandidateId         int
    LastLogIndex        int
    LastLogTerm         int
}

type RequestVoteReply struct {
    Term                int
    VoteGranted         bool
}

type AppendEntriesArgs struct {
    Term                int
    LeaderId            int
    PrevLogIndex        int
    PrevLogTerm         int
    Entries             []Entry
    LeaderCommit        int
}

type AppendEntriesReply struct {
    Term                int
    Success             bool
    RetryIndex          int
    RetryTerm           int
}

type InstallSnapshotArgs struct {
    Term                int
    LeaderId            int
    LastIncludeIndex    int
    LastIncludeTerm     int
    Offset              int
    Data                []byte
    Done                bool
}

type InstallSnapshotReply struct {
    Term                int
}

const (
    REQUEST_VOTE        = 1
    REQUEST_APPEND      = 2
    REQUEST_SNAPSHOT    = 3
)

type AsyncRequest struct {
    server              int
    requestType         int
    retryCnt            int
    body                interface{}
    replyCh             *chan AsyncReply
    chRef               *int32
}

type AsyncReply struct {
    server              int
    body                interface{}
    success             bool
}

/* fixing request reply caused by async requesting */
type Broadcaster struct {
    rf                  *Raft
    requestCh           map[int](*chan AsyncRequest)
    chCap               map[int](*int)
    mu                  sync.Mutex
    active              bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (this *Raft) GetState() (term int, isLeader bool) {
	// Your code here (3A).
    this.mu.Lock()
    term = this.currentTerm
    isLeader = this.votedFor == this.me
    this.mu.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (this *Raft) persist() {
    this.debug("PERSIST, this.log: %v\n", this.log)
    buf := new(bytes.Buffer)
    encoder := labgob.NewEncoder(buf)
    encoder.Encode(this.currentTerm)
    encoder.Encode(this.votedFor)
    encoder.Encode(this.log)
    encoder.Encode(this.snapshotTerm)
    encoder.Encode(this.snapshotIndex)
    this.persister.Save(buf.Bytes(), this.snapshot)
}

// restore previously persisted state.
func (this *Raft) readPersist(data []byte) (err error) {
    err = nil
    var buf *bytes.Buffer
    var decoder *labgob.LabDecoder
    var curTerm int
    var votedFor int
    var snapshotTerm int
    var snapshotIndex int
    if data == nil || len(data) < 1 { // bootstrap without any state?
        goto end
    }
    buf = bytes.NewBuffer(data)
    decoder = labgob.NewDecoder(buf)
    err = decoder.Decode(&curTerm)
    if (err != nil) {
        this.debug("decode currentTerm failed\n")
        goto end
    }
    err = decoder.Decode(&votedFor)
    if (err != nil) {
        this.debug("decode votedFor failed\n")
        goto end
    }
    err = decoder.Decode(&this.log)
    if (err != nil) {
        this.debug("decode log failed\n")
        goto end
    }
    err = decoder.Decode(&snapshotTerm)
    if (err != nil) {
        this.debug("decode snapshot term faield\n")
        goto end
    }
    err = decoder.Decode(&snapshotIndex)
    if (err != nil) {
        this.debug("decode snapshot index faield\n")
        goto end
    }
    this.currentTerm = curTerm
    this.votedFor = votedFor
    if (this.votedFor == this.me) {
        this.votedFor = -1
    }
    this.commitIndex = snapshotIndex
    for _, e := range(this.log) {
        this.commitIndex = e.Index
    }
    this.snapshotTerm = snapshotTerm
    this.snapshotIndex = snapshotIndex
    this.snapshot = this.persister.ReadSnapshot()
    this.debug("Initiated from persistent, commitIndex: %v, this.log: %v\n", this.commitIndex, this.log)

end:
    return
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (this *Raft) Snapshot(index int, snapshot []byte) {
    if (this.snapshotIndex >= index) {
        return
    }
    compactIndex := -1
    for idx, e := range(this.log) {
        if (e.Index == index) {
            compactIndex = idx
        }
    }
    this.snapshot = snapshot
    this.snapshotTerm = this.log[compactIndex].Term
    this.snapshotIndex = this.log[compactIndex].Index
    this.log = this.log[compactIndex + 1 : ]
    this.persist()

    this.debug("---- SNAPSHOT called ----- index: %v\n" +
               "====> this.snapshotIndex: %v, this.snapshotTerm: %v\n" +
               "====> this.log: %v\n" +
               "\n",
               index,
               this.snapshotIndex, this.snapshotTerm,
               this.log)
}

func (this *Raft) getLogBy(term int, index int) (entry *Entry, entryIndex int) {
    entry = nil
    for idx, e := range(this.log) {
        if (e.Term == term && e.Index == index) {
            entry = &e
            entryIndex = idx
        }
    }
    if (entry != nil) {
        return
    }
    if (term == 1 && index == 0) {
        /* initial empty log entry */
        return &Entry { Index: 0, Term: 1 }, -1
    } else {
        return nil, -1
    }
}

func (this *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    this.mu.Lock()
    defer this.mu.Unlock()

    /* leadership checking */
    if (this.currentTerm > args.Term) {
        this.debug("append denied\n")
        reply.Term = this.currentTerm
        reply.Success = false
        return
    }
    if (this.votedFor != args.LeaderId || this.currentTerm != args.Term) {
        /* a leader has been chosen by MAJORITY when append is called */
        this.votedFor = args.LeaderId
        this.currentTerm = args.Term
        this.persist()
    }
    reply.Term = args.Term
    /* log matching */
    e, truncateIdx := this.getLogBy(args.PrevLogTerm, args.PrevLogIndex)
    isMatch := e != nil || (this.snapshotIndex == args.PrevLogIndex && this.snapshotTerm == args.PrevLogTerm)
    if (isMatch) {
        if (e != nil) {
            /* truncate logs */
            this.log = this.log[0 : truncateIdx + 1]
        } else {
            this.log = []Entry{}
        }
        /* commit logs */
        this.log = append(this.log, args.Entries...)
        this.commitIndex = args.LeaderCommit
        /* apply log */
        if (args.LeaderCommit == args.PrevLogIndex) {
            /* leader has commited all logs */
            this.debug("follower commit logs\n")
            this.apply(this.currentTerm, this.commitIndex)
        }
        this.persist()
        reply.Success = true
    } else {
        reply.Success = false
        reply.RetryIndex = this.snapshotIndex
        reply.RetryTerm = this.snapshotTerm
        for _, entry := range(this.log) {
            if (entry.Term < args.PrevLogTerm) {
                reply.RetryIndex = entry.Index
                reply.RetryTerm = entry.Term
            }
        }
        this.debug("append retry\n")
    }

    this.debug("---- APPEND called ----\n" +
               "===> args, leader: %v, leader cur state [%v, %v], prev log [%v %v], entries: %v\n" +
               "===> reply, term: %v, success: %v, retry index: %v, retry term: %v\n" +
               "===> this.snapshotTerm: %v, this.snapshotIndex: %v\n" +
               "===> this.log: %v\n" +
               "\n",
               args.LeaderId, args.Term, args.LeaderCommit, args.PrevLogTerm, args.PrevLogIndex, args.Entries,
               reply.Term, reply.Success, reply.RetryIndex, reply.RetryTerm,
               this.snapshotTerm, this.snapshotIndex,
               this.log)
}

func (this *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    this.mu.Lock()
    defer this.mu.Unlock()

    lastLogTerm := this.snapshotTerm
    lastLogIndex := this.snapshotIndex
    for _, entry := range(this.log) {
        if (entry.Index == this.commitIndex) {
            lastLogTerm = entry.Term
            lastLogIndex = entry.Index
        }
    }
    reply.VoteGranted = this.currentTerm < args.Term &&
                            ((args.LastLogTerm > lastLogTerm) ||
                                (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex))
    if (reply.VoteGranted) {
        this.currentTerm = args.Term
        this.votedFor = -1
        reply.Term = args.Term
        this.persist()
    } else {
        this.debug("vote denied\n")
        reply.Term = this.currentTerm
    }

    this.debug("---- VOTE called ----\n" +
               "===> args, term: %v, candidate: %v, last log [%v %v]\n" +
               "===> reply, term: %v, granted: %v\n" +
               "===> this.log: %v\n" +
               "\n",
               args.Term, args.CandidateId, args.LastLogTerm, args.LastLogIndex,
               reply.Term, reply.VoteGranted,
               this.log)
}

func (this *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    this.mu.Lock()
    defer this.mu.Unlock()

    reply.Term = this.currentTerm
    if (args.Term < this.currentTerm) {
        this.debug("snapshot denied\n")
        return
    }
    /* refresh waiting for heartbeat */
    this.votedFor = args.LeaderId
    this.snapshotTerm = args.LastIncludeTerm
    this.snapshotIndex = args.LastIncludeIndex
    match, idx := this.getLogBy(this.snapshotTerm, this.snapshotIndex)
    if (match != nil) {
        /* compact logs before snapshot index */
        this.log = this.log[idx + 1 : ]
    } else {
        this.log = []Entry{}
        this.commitIndex = this.snapshotIndex
    }
    this.snapshot = args.Data
    this.persist()
    this.applySnapshot()

    this.debug("---- INSTALL SNAPSHOT called ----\n" +
               "===> args, term: %v, leader: %v, last snapshot log [%v %v]\n" +
               "===> reply, term: %v\n" +
               "===> this.log: %v\n" +
               "\n",
               args.Term, args.LeaderId, args.LastIncludeTerm, args.LastIncludeIndex,
               reply.Term,
               this.log)
}

func (this *Raft) rpc(server int, cmd string, args interface{}, reply interface{}) (success bool) {
    retCh := make(chan bool, 1)
    go func (server int, cmd string, args interface{}, reply interface{}, retCh *chan bool) {
        *retCh <- this.peers[server].Call(cmd, args, reply)
        close(*retCh)
    } (server ,cmd, args, reply, &retCh)

    select {
    case success = <- retCh:
    case <- time.After(time.Duration(RPC_TIMEOUT_MS) * time.Millisecond):
        this.debug("rpc timeout\n")
        success = false
    }
    return
}

func (this *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (success bool) {
    return this.rpc(server, "Raft.RequestVote", args, reply)
}

func (this *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (success bool) {
    return this.rpc(server, "Raft.AppendEntries", args, reply)
}

func (this *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs,
                                                  reply *InstallSnapshotReply) (success bool) {
    return this.rpc(server, "Raft.InstallSnapshot", args, reply)
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
func (this *Raft) Start(command interface{}) (index int, term int, success bool) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (this.votedFor != this.me) {
        success = false
        return
    }
    this.commitIndex++
    index = this.commitIndex
    term = this.currentTerm
    entry := Entry {
        Term    : term,
        Index   : index,
        Val     : command,
    }
    this.log = append(this.log, entry)
    this.persist()
    success = true

    this.debug("---- START called ----- command: %v\n" +
               "====> this.log: %v\n" +
               "\n",
               command,
               this.log)
    return
}

func (this *Raft) getLogsBetween(startIdx int, endIdx int) (entries []Entry) {
    if (endIdx == this.snapshotIndex) {
        /* last entry is snapshot entry */
        entries = append(entries, Entry{ Index: this.snapshotIndex, Term: this.snapshotTerm })
        return
    }
    if (startIdx == this.snapshotIndex) {
        /* first is snapshot entry */
        entries = append(entries, Entry{ Index: this.snapshotIndex, Term: this.snapshotTerm })
    }
    for _, e := range(this.log) {
        if (e.Index >= startIdx && e.Index <= endIdx) {
            entries = append(entries, e)
        }
    }
    return
}

func (this *Raft) applySnapshot() {
    if (len(this.snapshot) == 0) {
        return
    }
    this.applyCh <- ApplyMsg {
        CommandValid        : false,
        SnapshotValid       : true,
        Snapshot            : this.snapshot,
        SnapshotTerm        : this.snapshotTerm,
        SnapshotIndex       : this.snapshotIndex,
    }
    this.lastApplied = this.snapshotIndex
    this.debug("apply snapshot, snapshot term: %v, index: %v\n", this.snapshotTerm, this.snapshotIndex)
}

func (this *Raft) apply(curTerm int, commitIndex int) {
    entries := this.getLogsBetween(this.lastApplied, commitIndex)[1:]
    if (len(entries) == 0) {
        return
    }
    if (entries[len(entries) - 1].Term != curTerm) {
        this.debug("skipping commit of logs from previous term: %v\n", entries)
        return
    }
    for _, entry := range(entries) {
        msg := ApplyMsg {
            CommandValid        : true,
            Command             : entry.Val,
            CommandIndex        : entry.Index,
            SnapshotValid       : false,
        }
        this.applyCh <- msg
    }
    this.lastApplied = commitIndex
    this.debug("commit logs: %v\n", entries)
}

func (this *Raft) requestVoteFrom(server int, term int, replyCh *chan AsyncReply, chRef *int32) {
    entries := this.getLogsBetween(this.commitIndex, this.commitIndex)
    args := RequestVoteArgs {
        Term            : term,
        CandidateId     : this.me,
        LastLogTerm     : this.snapshotTerm,
        LastLogIndex    : this.snapshotIndex,
    }
    if (len(entries) != 0) {
        args.LastLogTerm = entries[0].Term
        args.LastLogIndex = entries[0].Index
    }
    this.broadcaster.send(server, AsyncRequest {
            server          : server,
            requestType     : REQUEST_VOTE,
            retryCnt        : RPC_RETRY_VOTE,
            body            : args,
            replyCh         : replyCh,
            chRef           : chRef,
    })
    return
}

func (this *Raft) elect(term int) (elected bool, newTerm int) {
    broadcastCnt := len(this.peers) - 1
    replyCh := make(chan AsyncReply, broadcastCnt)
    chRef := int32(broadcastCnt)
    sendCnt := 0
    for server, _ := range(this.peers) {
        if (server == this.me) {
            continue
        }
        this.requestVoteFrom(server, term, &replyCh, &chRef)
        sendCnt++
    }
    granted := 1
    majority := broadcastCnt / 2 + 1
    for i := 0; i < sendCnt && !this.killed() && granted != majority; i++ {
        reply := <-replyCh
        if (!reply.success) {
            continue
        }
        body := reply.body.(RequestVoteReply)
        if (body.VoteGranted) {
            granted++
        } else if (body.Term >= term) {
            newTerm = body.Term
            break
        }
    }
    elected = granted == majority
    if (elected) {
        newTerm = term
    }
    return
}

func (this *Raft) replicateTo(server int, term int, startIdx int, endIdx int, replyCh *chan AsyncReply, chRef *int32) {
    entries := this.getLogsBetween(startIdx, endIdx)
    args := AppendEntriesArgs {
        Term            : term,
        LeaderId        : this.me,
        PrevLogIndex    : entries[0].Index,
        PrevLogTerm     : entries[0].Term,
        Entries         : entries[1 : ],
        LeaderCommit    : endIdx,
    }
    this.broadcaster.send(server, AsyncRequest {
        server          : server,
        requestType     : REQUEST_APPEND,
        retryCnt        : RPC_RETRY_APPEND,
        body            : args,
        replyCh         : replyCh,
        chRef           : chRef,
    })
    return
}

func (this *Raft) installSnapshotTo(server int, curTerm int) (success bool, newTerm int) {
    if (this.snapshot == nil) {
        success = true
        newTerm = curTerm
        return
    }
    args := InstallSnapshotArgs {
        Term                : curTerm,
        LeaderId            : this.me,
        LastIncludeIndex    : this.snapshotIndex,
        LastIncludeTerm     : this.snapshotTerm,
        Offset              : 0,
        Data                : this.snapshot,
        Done                : true,
    }
    replyCh := make(chan AsyncReply, 1)
    chRef := int32(1)
    this.broadcaster.send(server, AsyncRequest {
        server              : server,
        requestType         : REQUEST_SNAPSHOT,
        retryCnt            : RPC_RETRY_SNAPSHOT,
        body                : args,
        replyCh             : &replyCh,
        chRef               : &chRef,
    })
    reply := <- replyCh
    if (!reply.success) {
        success = false
        newTerm = -1
        return
    }
    newTerm = reply.body.(InstallSnapshotReply).Term
    success = newTerm == curTerm
    return
}

func (this *Raft) replicate(term int, index int) (success bool) {
    this.debug("==== replicate logs of term: %v, index: %v start\n", term, index)
    broadcastCnt := len(this.peers) - 1
    replyCh := make(chan AsyncReply, broadcastCnt)
    chRef := int32(broadcastCnt)
    sendCnt := 0
    for server, _ := range(this.peers) {
        if (server == this.me) {
            continue
        }
        this.replicateTo(server, term, this.nextIndex[server], index, &replyCh, &chRef)
        sendCnt++
    }
    granted := 1
    majority := broadcastCnt / 2 + 1
    for i := 0; i < sendCnt && !this.killed(); i++ {
        reply := <- replyCh
        if (!reply.success) {
            continue
        }
        body := reply.body.(AppendEntriesReply)
        if (body.Success) {
            this.nextIndex[reply.server] = index
            granted++
        } else {
            if (body.Term == term) {
                retryIndex := this.snapshotIndex
                entries := this.getLogsBetween(this.snapshotIndex, body.RetryIndex)
                for _, entry := range(entries) {
                    if (entry.Term <= body.RetryTerm) {
                        retryIndex = entry.Index
                    }
                }
                /* install snapshot */
                if (retryIndex == this.snapshotIndex && len(this.snapshot) > 0) {
                    this.debug("install snapshot to %v\n", reply.server)
                    snapshoted, newTerm := this.installSnapshotTo(reply.server, term)
                    if (!snapshoted) {
                        if (newTerm > term) {
                            this.debug("snapshot failed, update term from %v to %v\n", term, newTerm)
                            this.currentTerm = newTerm
                            break
                        } else {
                            this.debug("install snapshot to %v failed\n")
                            continue
                        }
                    }
                }
                this.debug("retry replicate to %v, with previous index %v, this.snapshotIndex: %v\n", reply.server, retryIndex, this.snapshotIndex)
                this.nextIndex[reply.server] = retryIndex
                this.replicateTo(reply.server, term, this.nextIndex[reply.server], index, &replyCh, &chRef)
                sendCnt++
                continue
            } else {
                this.debug("replicate log failed, update term from %v to %v\n", body.Term, term)
                this.currentTerm = body.Term
                break
            }
        }
    }
    success = granted >= majority
    this.debug("==== replicate logs of term: %v, index: %v done, success: %v\n", term, index, success)
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
func (this *Raft) Kill() {
    this.debug("killing, this.log: %v\n", this.log)
	atomic.StoreInt32(&this.dead, 1)
    this.broadcaster.final()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (this *Raft) wait4Heartbeat() {
    checked := true
    for !this.killed() && checked {
        this.mu.Lock()
        this.votedFor = -1
        term := this.currentTerm
        this.mu.Unlock()
        time.Sleep(time.Duration(HEARTBEAT_TIMEOUT_MS_BASE +
                                (rand.Int63() % HEARTBEAT_TIMEOUT_MS_DELTA)) * time.Millisecond)
        this.mu.Lock()
        checked = this.votedFor != -1 || this.currentTerm > term
        this.mu.Unlock()
    }
}

func (this *Raft) startTerm() (success bool, term int) {
    this.mu.Lock()
    electTerm := this.currentTerm + 1
    this.debug("start election for term: %v, this.log: %v\n", electTerm, this.log)
    success, term = this.elect(electTerm)
    if (success) {
        this.votedFor = this.me
        this.currentTerm = electTerm
        this.persist()
        /* init next index */
        for server, _ := range(this.peers) {
            if (server == this.me) {
                continue
            }
            this.nextIndex[server] = this.commitIndex
            this.matchIndex[server] = 0
        }
        this.debug("election success\n")
    } else {
        this.votedFor = -1
        if (term > this.currentTerm) {
            this.debug("election failed, update term from %v to %v\n", this.currentTerm, term)
            this.currentTerm = term
        }
    }
    this.mu.Unlock()
    return
}

func (this *Raft) heartbeat() {
    this.debug("heartbeat started\n")
    for !this.killed() {
        this.mu.Lock()
        if (this.votedFor != this.me) {
            this.mu.Unlock()
            break
        }
        term := this.currentTerm
        index := this.commitIndex
        if (!this.replicate(term, index)) {
            this.votedFor = -1
            this.persist()
            this.mu.Unlock()
            break
        }
        this.apply(term, index)
        this.mu.Unlock()
        /* hearbeat broadcast happens every 100ms */
        time.Sleep(time.Duration(HEARTBEAT_TICK_MS) * time.Millisecond)
    }
    this.debug("heartbeat terminated\n")
}

func (this *Raft) ticker() {
    this.applySnapshot()
	for !this.killed() {
        curTerm, isLeader := this.GetState()
        if (!isLeader) {
            this.debug("====> FOLLOWER\n")
            this.wait4Heartbeat()
            if (!this.killed()) {
                this.debug("====> CANDIDATE\n")
                isLeader, curTerm = this.startTerm()
                if (!isLeader) {
                    continue
                }
            }
        }
        this.debug("====> LEADER term: %v\n", curTerm)
        this.heartbeat()
	}
}

func handleRequest(request *AsyncRequest, rf *Raft) (reply AsyncReply) {
    reply.server = request.server
    for i := 0; i < request.retryCnt; i++ {
        var task string
        switch (request.requestType) {
        case REQUEST_VOTE:
            task = "voting"
            requestBody := request.body.(RequestVoteArgs)
            replyBody := RequestVoteReply{}
            reply.success = rf.sendRequestVote(request.server, &requestBody, &replyBody)
            reply.body = replyBody
        case REQUEST_APPEND:
            task = "append"
            requestBody := request.body.(AppendEntriesArgs)
            replyBody := AppendEntriesReply{}
            reply.success = rf.sendAppendEntries(request.server, &requestBody, &replyBody)
            reply.body = replyBody
        case REQUEST_SNAPSHOT:
            task = "snapshot"
            requestBody := request.body.(InstallSnapshotArgs)
            replyBody := InstallSnapshotReply{}
            reply.success = rf.sendInstallSnapshot(request.server, &requestBody, &replyBody)
            reply.body = replyBody
        default:
            rf.debug("request type: %v, check your coding\n", request.requestType)
            panic("bad request type\n")
        }
        rf.debug("[broadcast] %v to %v\n===> args: %v\n===> reply: %v\n", task, request.server, *request, reply)
        if (reply.success) {
            break
        }
    }
    return
}

func handleBroadcastTo(server int, rf *Raft, requestCh chan AsyncRequest) {
    rf.debug("[broadcast] handler of %v start\n", server)
    for request := range(requestCh) {
        reply := handleRequest(&request, rf)
        if (!reply.success) {
            rf.debug("[broadcast] request to %v failed due to network issues\n", server)
        }
        (*request.replyCh) <- reply
        if (request.requestType == REQUEST_APPEND && reply.success) {
            /* retry case */
            body := reply.body.(AppendEntriesReply)
            if (!body.Success && body.Term == request.body.(AppendEntriesArgs).Term) {
                continue
            }
        }
        if (atomic.AddInt32(request.chRef, -1) == 0) {
            close(*request.replyCh)
        }
    }
    rf.debug("[broadcast] handler of %v finished\n", server)
}

func (this *Broadcaster) init(rf *Raft) {
    this.rf = rf
    this.mu = sync.Mutex{}
    this.active = true
    this.requestCh = make(map[int](*chan AsyncRequest))
    this.chCap = make(map[int](*int))
    for server, _ := range(rf.peers) {
        if (server != rf.me) {
            requestCh := make(chan AsyncRequest)
            this.requestCh[server] = &requestCh
            go handleBroadcastTo(server, rf, requestCh)
        }
    }
}

func (this *Broadcaster) final() {
    this.mu.Lock()
    defer this.mu.Unlock()

    for server, _ := range(this.rf.peers) {
        if (server != this.rf.me) {
            ch := this.requestCh[server]
            close(*ch)
        }
    }
    this.active = false
}

func (this *Broadcaster) send(server int, request AsyncRequest) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (!this.active) {
        return
    }
    ch := this.requestCh[server]
    (*ch) <- request
    return
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
	rf := &Raft {
        mu                  : sync.Mutex{},
        peers               : peers,
        persister           : persister,
        me                  : me,
        /* protocol properties */
        currentTerm         : 0,
        votedFor            : -1,
        log                 : []Entry{},
        commitIndex         : 0,
        lastApplied         : 0,
        nextIndex           : make(map[int]int),
        matchIndex          : make(map[int]int),
        /* implementing properties */
        applyCh             : applyCh,
        broadcaster         : Broadcaster{},
        snapshot            : []byte{},
        snapshotTerm        : 1,
        snapshotIndex       : 0,
    }
    rf.broadcaster.init(rf)

	// initialize from state persisted before a crash
	err := rf.readPersist(persister.ReadRaftState())
    if (err != nil) {
        fmt.Printf("read persistent data failed %v\n", err)
        panic("")
    }

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
