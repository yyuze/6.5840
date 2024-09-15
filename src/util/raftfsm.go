package util

import "6.5840/labrpc"
import "6.5840/labgob"
import "6.5840/raft"

import "sync"
import "sync/atomic"
import "fmt"
import "bytes"
import "time"
import "log"

const DEBUG = false

func (this *RaftFSM) debug(format string, a ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[server %v] ", this.id)
		log.Printf(prefix + format, a...)
	}
	return
}

type OpResult struct {
    body                interface{}
}

type Op struct {
    StartServer         int
    Serial              uint64
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
        fmt.Printf("%v\n", err)
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
    function            func(data interface{}) (reply interface{})
}

type RaftSnapshotSerializer func() (snapshot []byte)

type RaftSnapshotDeserializer func(snapshot []byte)

type RaftFSM struct {
    id                          int
    commitCh                    chan raft.ApplyMsg
    persister                   *raft.Persister
    maxraftstate                int
    rf                          *raft.Raft
    mu                          sync.Mutex
    handlers                    map[uint32]RaftHandler
    snapshotSerializer          RaftSnapshotSerializer
    snapshotDeserializer        RaftSnapshotDeserializer
    maxClerkSerial              map[int64]uint64
    opSerial                    uint64
    committing                  map[uint64](*chan OpResult)
}

func (this *RaftFSM) Init(id int, servers []*labrpc.ClientEnd, persister *raft.Persister, maxraftstate int) {
    this.id = id
    this.commitCh = make(chan raft.ApplyMsg, 1)
    this.persister = persister
    this.maxraftstate = maxraftstate
    this.rf = raft.Make(servers, id, persister, this.commitCh)
    this.mu = sync.Mutex{}
    this.handlers = make(map[uint32]RaftHandler)
    this.snapshotSerializer = nil
    this.snapshotDeserializer = nil
    this.maxClerkSerial = make(map[int64]uint64)
    this.opSerial = uint64(0)
    this.committing = make(map[uint64](*chan OpResult))
}

func (this *RaftFSM) Raft() *raft.Raft {
    return this.rf
}

func (this *RaftFSM) RegisterHandler(opCode uint32, readonly bool,
                                     function func(args interface{}) (reply interface{})) {
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
    resultCh := make(chan OpResult, 1)
    op := Op {
        StartServer     : this.id,
        Serial          : atomic.AddUint64(&this.opSerial, 1),
        ClerkId         : clerkId,
        ClerkSerial     : clerkSerial,
        Type            : opType,
        Body            : data,
    }
    this.mu.Lock()
    this.committing[op.Serial] = &resultCh
    this.mu.Unlock()
    /* start op on raft FSM */
    index, term, success := this.rf.Start(op.toBytes())
    if (!success) {
        this.debug("%v is not leader\n", this.id)
        return
    }
    /* wait result with timeout */
    select {
    case opResult := <- resultCh:
        result = opResult.body
        success = true
    case <-time.After(time.Duration(2 * raft.HEARTBEAT_TICK_MS) * time.Millisecond):
        this.debug("start op timeout, term: %v, index: %v\n", term, index)
        success = false
    }
    this.mu.Lock()
    close(resultCh)
    delete(this.committing, op.Serial)
    this.mu.Unlock()
    return
}

func (this *RaftFSM) commandHandler(command []byte) {
    op := Op{}
    op.fromBytes(command)
    this.mu.Lock()
    /* check whether op is already processed */
    var processed bool
    maxSerial, contains := this.maxClerkSerial[op.ClerkId]
    if (contains && maxSerial >= op.ClerkSerial) {
        processed = true
    } else {
        this.maxClerkSerial[op.ClerkId] = op.ClerkSerial
        processed = false
    }
    /* get op handler */
    handler, contains := this.handlers[op.Type]
    if (!contains) {
        fmt.Printf("op.Type: %v\n", op.Type)
        panic("illegal unregister operation type\n")
    }
    this.mu.Unlock()
    /* handle op */
    result := OpResult{}
    if (!processed || handler.readonly) {
        result.body = handler.function(op.Body)
    }
    /* waitup waiting thread */
    if (op.StartServer == this.id) {
        this.mu.Lock()
        /* maybe raft-start-timeout before send result to channel */
        ch, waiting := this.committing[op.Serial]
        if (waiting) {
            (*ch) <- result
        }
        this.mu.Unlock()
    }
}

type Snapshot struct {
    Data                []byte
    MaxClerkSerial      map[int64]uint64
}

func (this *RaftFSM) takeSnapshot(index int) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (this.snapshotSerializer == nil) {
        panic("snapshot serializer is not regisered, cannot serialize snapshot\n")
    }
    snapshot := Snapshot {
        Data            : this.snapshotSerializer(),
        MaxClerkSerial  : this.maxClerkSerial,
    }
    buf := bytes.Buffer{}
    enc := labgob.NewEncoder(&buf)
    enc.Encode(&snapshot)
    this.debug("made snapshot for raft index: %v\n", index)

    go this.rf.Snapshot(index, buf.Bytes())
}

func (this *RaftFSM) applySnapshot(snapshotBytes []byte) {
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
        panic("decode snapshot failed=n")
    }
    this.snapshotDeserializer(snapshot.Data)
    this.maxClerkSerial = snapshot.MaxClerkSerial
}

func (this *RaftFSM) handler() {
    for msg := range(this.commitCh) {
        if (msg.CommandValid) {
            this.debug("handle raft operaion on index: %v\n", msg.CommandIndex)
            this.commandHandler(msg.Command.([]byte))
            if (this.maxraftstate != -1 && this.persister.RaftStateSize() > this.maxraftstate) {
                this.takeSnapshot(msg.CommandIndex)
            }
        } else if (msg.SnapshotValid) {
            if (this.snapshotDeserializer == nil) {
                panic("snapshot handler is not registered\n")
            }
            this.debug("handle raft snapthot on term: %v, index: %v\n", msg.SnapshotTerm, msg.SnapshotIndex)
            this.applySnapshot(msg.Snapshot)
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
