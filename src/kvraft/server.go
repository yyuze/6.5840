package kvraft

import (
    "6.5840/labgob"
    "6.5840/labrpc"
    "6.5840/raft"
    "log"
    "sync"
    "sync/atomic"
    "time"
    "bytes"
    "fmt"
    "sort"
)

func (this *KVServer) debug(format string, a ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[server %v] ", this.me)
		log.Printf(prefix + format, a...)
	}
	return
}

type Op struct {
    StartServer     int
    Type            int
    ClerkId         int64
    Timestamp       int64
    Serial          int64
    Key             string
    Value           string
}

type CommitState struct {
    done            chan bool
    oldVal          *string
    contains        *bool
}

const (
    OP_GET          = 1
    OP_PUT          = 2
    OP_APPEND       = 3
)

type Value struct {
    Timestamp       int64
    Serial          int64
    ClerkId         int64
    Data            string
}

type Values []Value

func (arr Values) Len() int {
    return len(arr)
}

func (arr Values) Swap(i int, j int) {
    temp := arr[j]
    arr[j] = arr[i]
    arr[i] = temp
}

func (arr Values) Less(i int, j int) bool {
    return (arr[i].Timestamp < arr[j].Timestamp) ||
           (arr[i].Timestamp == arr[j].Timestamp && arr[i].ClerkId < arr[j].ClerkId) ||
           (arr[i].Timestamp == arr[j].Timestamp && arr[i].ClerkId == arr[j].ClerkId && arr[i].Serial < arr[j].Serial)
}

/* clerkid -> { serial, data } */
type Version map[int64]([]Value)

type KVServer struct {
    mu              sync.Mutex
    me              int
    rf              *raft.Raft
    commitCh        chan raft.ApplyMsg
    dead            int32

    maxraftstate    int // snapshot if log grows this big

    // Your definitions here.
	persister       *raft.Persister
    data            map[string]Version
    committing      map[int](*CommitState)
    maxSerial       map[int64]int64
}

func (this *KVServer) sync(index int) (success bool, contains bool, value string) {
    /* put into wait map */
    this.mu.Lock()
    state := CommitState {
        done        : make(chan bool, 1),
        oldVal      : &value,
        contains    : &contains,
    }
    this.committing[index] = &state
    this.mu.Unlock()
    /* wait for complete */
    select {
    case <-state.done:
        success = true
    case <-time.After(time.Duration(2 * raft.HEARTBEAT_TICK_MS) * time.Millisecond):
        success = false
        this.debug("server %v wait for committing index %v timeout\n", this.me, index)
    }
    this.mu.Lock()
    delete(this.committing, index)
    this.mu.Unlock()
    return
}

func (this *KVServer) startReplicate(op int, clerkId int64, serial int64,
                                     key string, value string) (success bool, contains bool, oldVal string) {
    buf := bytes.Buffer{}
    encoder := labgob.NewEncoder(&buf)
    encoder.Encode(Op{
        StartServer     : this.me,
        Type            : op,
        ClerkId         : clerkId,
        Timestamp       : time.Now().UnixMilli(),
        Serial          : serial,
        Key             : key,
        Value           : value,
    })
    index, term, success := this.rf.Start(buf.Bytes())
    if (!success) {
        this.debug("%v is not leader\n", this.me)
        contains = false
        oldVal = ""
        return
    }
    success, contains, oldVal = this.sync(index)
    if (!success) {
        this.debug("%v is not leader, leadership may be changed, term: %v\n", this.me, term)
        contains = false
        oldVal = ""
        return
    }
    return
}

func (this *KVServer) Get(args *GetArgs, reply *GetReply) {
    success, contains, value := this.startReplicate(OP_GET, args.ClerkId, args.Serial, args.Key, "")
    if (success) {
        if (!contains) {
            reply.Err = ErrNoKey
        } else {
            reply.Err = OK
        }
        reply.Value = value
    } else {
        reply.Err = ErrWrongLeader
        reply.Value = ""
    }
    return
}

func (this *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
    success, _, _ := this.startReplicate(OP_PUT, args.ClerkId, args.Serial, args.Key, args.Value)
    if (success) {
        reply.Err = OK
    } else {
        reply.Err = ErrWrongLeader
    }
    return
}

func (this *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
    success, _, _ := this.startReplicate(OP_APPEND, args.ClerkId, args.Serial, args.Key, args.Value)
    if (success) {
        reply.Err = OK
    } else {
        reply.Err = ErrWrongLeader
    }
    return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (this *KVServer) Kill() {
    atomic.StoreInt32(&this.dead, 1)
    this.rf.Kill()
}

func (this *KVServer) killed() bool {
    return atomic.LoadInt32(&this.dead) == 1
}

func (this *KVServer) kvGet(key string) (contains bool, value string) {
    ver, contains := this.data[key]
    if (!contains) {
        return
    }
    values := []Value{}
    for _, v := range(ver) {
        values = append(values, v...)
    }
    sort.Sort(Values(values))
    value = ""
    for _, v := range(values) {
        this.debug("getting timestamp: %v, serial: %v, clerkId: %v, data: %v\n",
                   v.Timestamp, v.Serial, v.ClerkId, v.Data)
        value += v.Data
    }
    return
}

func (this *KVServer) kvPut(clerkId int64, timestamp int64, serial int64, key string, value string) {
    this.data[key] = make(Version)
    this.data[key][clerkId] = []Value {
        Value {
            Timestamp   : 0,
            Serial      : serial,
            ClerkId     : clerkId,
            Data        : value,
        },
    }
}

func (this *KVServer) kvAppend(clerkId int64, timestamp int64, serial int64, key string, value string) {
    ver, contains := this.data[key]
    if (!contains) {
        this.kvPut(clerkId, timestamp, serial, key, value)
        return
    }
    oldVal, contains := ver[clerkId]
    if (!contains) {
        ver[clerkId] = []Value {
            Value {
                Timestamp   : timestamp,
                Serial      : serial,
                ClerkId     : clerkId,
                Data        : value,
            },
        }
        return
    }
    ver[clerkId] = append(oldVal, Value {
        Timestamp   : timestamp,
        Serial      : serial,
        ClerkId     : clerkId,
        Data        : value,
    })
    return
}

func (this *KVServer) processed(op Op) (ret bool) {
    serial, contains := this.maxSerial[op.ClerkId]
    ret = contains && serial >= op.Serial
    if (!ret) {
        /* not processed */
        this.maxSerial[op.ClerkId] = op.Serial
    }
    return
}

func (this *KVServer) snapshot(raftIndex int) {
    if (this.maxraftstate == -1) {
        return
    }
    if (this.maxraftstate > this.persister.RaftStateSize()) {
        return
    }
    buf := bytes.Buffer{}
    enc := labgob.NewEncoder(&buf)
    enc.Encode(this.data)
    enc.Encode(this.maxSerial)
    this.debug("made snapshot for raft index: %v\n", raftIndex)
    go this.rf.Snapshot(raftIndex, buf.Bytes())
}

func (this *KVServer) handleCommand(command interface{}, index int) {
    this.mu.Lock()
    defer this.mu.Unlock()

    /* decode op */
    buf := bytes.NewBuffer(command.([]byte))
    decoder := labgob.NewDecoder(buf)
    op := Op{}
    err := decoder.Decode(&op)
    if (err != nil) {
        fmt.Printf("%v\n", err)
        panic("decode failed, unexpected error\n")
    }
    processed := this.processed(op)
    /* process op */
    contains := false
    oldVal := ""
    opName := ""
    switch(op.Type) {
    case OP_GET:
        contains, oldVal = this.kvGet(op.Key)
        opName = "GET"
    case OP_PUT:
        if (!processed) {
            this.kvPut(op.ClerkId, op.Timestamp, op.Serial, op.Key, op.Value)
            contains = true
            opName = "PUT"
        }
    case OP_APPEND:
        if (!processed) {
            this.kvAppend(op.ClerkId, op.Timestamp, op.Serial, op.Key, op.Value)
            contains = true
            opName = "APPEND"
        }
    default:
        panic("unknown operation\n")
    }
    if (!processed || op.Type == OP_GET) {
        this.debug("%v %v->%v, clerkId: %v, timestamp: %v, serial: %v\n",
                   opName, op.Key, oldVal, op.ClerkId, op.Timestamp, op.Serial)
    }
    /* result notify */
    if (op.StartServer == this.me) {
        state, waiting := this.committing[index]
        if (waiting) {
            *state.contains = contains
            *state.oldVal = oldVal
            (*state).done <- true
            close((*state).done)
        }
    }
    this.snapshot(index)
    return
}

func (this *KVServer) handleSnapshot(snapshot []byte) {
    this.mu.Lock()
    defer this.mu.Unlock()

    buf := bytes.NewBuffer(snapshot)
    dec := labgob.NewDecoder(buf)
    err := dec.Decode(&this.data)
    if (err != nil) {
        fmt.Printf("%v\n", err)
        panic("decode data failed, unexpected error\n")
    }
    err = dec.Decode(&this.maxSerial)
    if (err != nil) {
        fmt.Printf("%v\n", err)
        panic("decode maxSerial failed, unexpected error\n")
    }

    this.debug("applied snapshot")
}

func (this *KVServer) commitHandler() {
    for msg := range(this.commitCh) {
        if (msg.CommandValid) {
            this.handleCommand(msg.Command, msg.CommandIndex)
        } else if (msg.SnapshotValid) {
            this.handleSnapshot(msg.Snapshot)
        } else {
            panic("invalid commit message\n")
        }
    }
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

    kv := &KVServer {
        mu                  : sync.Mutex{},
        me                  : me,
        maxraftstate        : maxraftstate,
        persister           : persister,
        data                : make(map[string]Version),
        committing          : make(map[int](*CommitState)),
        maxSerial           : make(map[int64]int64),
    }
    kv.commitCh = make(chan raft.ApplyMsg, 1)
    kv.rf = raft.Make(servers, me, persister, kv.commitCh)

    go kv.commitHandler()

	return kv
}
