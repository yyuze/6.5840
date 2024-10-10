package util

import "6.5840/labrpc"
import "6.5840/labgob"
import "6.5840/raft"

import "sync"
import "fmt"
import "bytes"
import "time"
import "log"
import "math/big"
import "crypto/rand"

const DEBUG = false

func (this *RaftFSM) debug(format string, a ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[server %v] ", this.me)
		log.Printf(prefix + format, a...)
	}
	return
}

type OpResult struct {
    Serial              uint64
    Body                interface{}
}

type OpHistory struct {
    Results             map[uint64]OpResult     /* clerk serial -> OpResult */
}

type Op struct {
    StartServer         int64
    ClerkId             int64
    ClerkSerial         uint64
    Type                uint32
    Body                interface{}
}

func (this *Op) fromBytes(data []byte) {
    buf := bytes.NewBuffer(data)
    dec := labgob.NewDecoder(buf)
    err := dec.Decode(this)
    if (err != nil) {
        fmt.Printf("op: %v, %v\n", *this, err)
        panic("decode raft operation failed\n")
    }
}

func (this *Op) toBytes() []byte {
    buf := bytes.Buffer{}
    encoder := labgob.NewEncoder(&buf)
    encoder.Encode(this)
    return buf.Bytes()
}

type RaftHandler struct {
    readonly            bool
    function            func(raftIndex int, data interface{}) (reply interface{})
}

type RaftSnapshotSerializer func(index int) (snapshot []byte)

type RaftSnapshotDeserializer func(index int, snapshot []byte)

type RaftFSM struct {
    me                          int
    id                          int64
    commitCh                    chan raft.ApplyMsg
    persister                   *raft.Persister
    maxraftstate                int
    rf                          *raft.Raft
    mu                          sync.Mutex
    handlers                    map[uint32]RaftHandler
    snapshotSerializer          RaftSnapshotSerializer
    snapshotDeserializer        RaftSnapshotDeserializer
    processed                   map[int64]OpHistory
    committing                  map[int](*chan OpResult)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (this *RaftFSM) Init(me int, servers []*labrpc.ClientEnd, persister *raft.Persister, maxraftstate int) {
    this.me = me
    this.id = nrand()
    this.commitCh = make(chan raft.ApplyMsg, 1)
    this.persister = persister
    this.maxraftstate = maxraftstate
    this.rf = raft.Make(servers, me, persister, this.commitCh)
    this.mu = sync.Mutex{}
    this.handlers = make(map[uint32]RaftHandler)
    this.snapshotSerializer = nil
    this.snapshotDeserializer = nil
    this.processed = make(map[int64]OpHistory)
    this.committing = make(map[int](*chan OpResult))
}

func (this *RaftFSM) Raft() *raft.Raft {
    return this.rf
}

func (this *RaftFSM) RegisterHandler(opCode uint32, readonly bool,
                                     function func(raftIndex int, args interface{}) (reply interface{})) {
    this.mu.Lock()
    defer this.mu.Unlock()

    this.handlers[opCode] = RaftHandler {
        readonly       : readonly,
        function       : function,
    }
}

func (this *RaftFSM) RegisterSnapshotHandler(serializer RaftSnapshotSerializer,
                                             deserializer RaftSnapshotDeserializer) {
    this.mu.Lock()
    defer this.mu.Unlock()

    this.snapshotSerializer = serializer
    this.snapshotDeserializer = deserializer
}

func (this *RaftFSM) Submit(clerkId int64, clerkSerial uint64, opType uint32,
                            data interface{}) (success bool, result interface{}) {
    this.mu.Lock()
    resultCh := make(chan OpResult, 1)
    op := Op {
        StartServer     : this.id,
        ClerkId         : clerkId,
        ClerkSerial     : clerkSerial,
        Type            : opType,
        Body            : data,
    }
    /* start op on raft FSM */
    index, term, success := this.rf.Start(op.toBytes())
    if (!success) {
        this.debug("%v is not leader\n", this.me)
        this.mu.Unlock()
        return
    }
    this.committing[index] = &resultCh
    this.mu.Unlock()
    /* wait result with timeout */
    select {
    case opResult := <- resultCh:
        result = opResult.Body
        success = true
    case <-time.After(time.Duration(2 * raft.HEARTBEAT_TICK_MS) * time.Millisecond):
        this.debug("start op timeout, term: %v, index: %v\n", term, index)
        success = false
    }
    this.mu.Lock()
    close(resultCh)
    delete(this.committing, index)
    this.mu.Unlock()
    return
}

func (this *RaftFSM) commandHandler(index int, command []byte) {
    op := Op{}
    op.fromBytes(command)
    /* get op handler */
    handler, contains := this.handlers[op.Type]
    if (!contains) {
        fmt.Printf("op.Type: %v\n", op.Type)
        panic("illegal unregister operation type\n")
    }
    /* check whether op is already processed */
    history, contains := this.processed[op.ClerkId]
    if (!contains) {
        history = OpHistory { Results: make(map[uint64]OpResult) }
    }
    prevResult, contains := history.Results[op.ClerkSerial]
    var result OpResult
    if (!contains || handler.readonly) {
        result.Body = handler.function(index, op.Body)
        result.Serial = op.ClerkSerial
        history.Results[op.ClerkSerial] = result
        this.processed[op.ClerkId] = history
    } else {
        result = prevResult
    }
    /* waitup waiting thread */
    if (op.StartServer == this.id) {
        this.mu.Lock()
        /* maybe raft-start-timeout before send result to channel */
        ch, waiting := this.committing[index]
        if (waiting) {
            (*ch) <- result
        }
        this.mu.Unlock()
    }
}

type Snapshot struct {
    Data                []byte
    Processed           map[int64]OpHistory
}

func (this *RaftFSM) takeSnapshot(index int) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (this.snapshotSerializer == nil) {
        panic("snapshot serializer is not regisered, cannot serialize snapshot\n")
    }
    snapshot := Snapshot {
        Data            : this.snapshotSerializer(index),
        Processed       : this.processed,
    }
    buf := bytes.Buffer{}
    enc := labgob.NewEncoder(&buf)
    enc.Encode(&snapshot)
    this.debug("made snapshot for raft index: %v\n", index)

    go this.rf.Snapshot(index, buf.Bytes())
}

func (this *RaftFSM) applySnapshot(snapshotIndex int, snapshotBytes []byte) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (this.snapshotDeserializer == nil) {
        panic("snapshot deserializer is not regisered, cannot deserialize snapshot\n")
    }
    snapshot := Snapshot{}
    buf := bytes.NewBuffer(snapshotBytes)
    dec := labgob.NewDecoder(buf)
    err := dec.Decode(&snapshot)
    if (err != nil) {
        fmt.Printf("%v\n", err)
        panic("decode snapshot failed\n")
    }
    this.snapshotDeserializer(snapshotIndex, snapshot.Data)
    this.processed = snapshot.Processed
}

func (this *RaftFSM) handler() {
    for msg := range(this.commitCh) {
        if (msg.CommandValid) {
            this.debug("handle raft operaion on index: %v\n", msg.CommandIndex)
            this.commandHandler(msg.CommandIndex, msg.Command.([]byte))
            if (this.maxraftstate != -1 && this.persister.RaftStateSize() > this.maxraftstate) {
                this.takeSnapshot(msg.CommandIndex)
            }
        } else if (msg.SnapshotValid) {
            if (this.snapshotDeserializer == nil) {
                panic("snapshot handler is not registered\n")
            }
            this.debug("handle raft snapthot on term: %v, index: %v\n", msg.SnapshotTerm, msg.SnapshotIndex)
            this.applySnapshot(msg.SnapshotIndex, msg.Snapshot)
        } else {
            panic("unknonw raft message type\n")
        }
    }
}

func (this *RaftFSM) Start() {
    go this.handler()
}

func (this *RaftFSM) Kill() {
    this.rf.Kill()
}
