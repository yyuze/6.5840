package shardkv

import "6.5840/labgob"
import "6.5840/labrpc"
import "6.5840/raft"
import "6.5840/shardctrler"
import "6.5840/util"

import "sync"
import "sync/atomic"
import "sort"
import "bytes"
import "fmt"
import "time"

type Version struct {
    RaftIndex           int
    ClerkId             int64
    Seq                 uint64
    Data                string
}

type Versions []Version

func (arr Versions) Len() int {
    return len(arr)
}

func (arr Versions) Swap(i int, j int) {
    temp := arr[j]
    arr[j] = arr[i]
    arr[i] = temp
}

func (arr Versions) Less(i int, j int) bool {
    return arr[i].RaftIndex < arr[j].RaftIndex
}

type Value struct {
    mutates             map[int64]([]Version)                   /* clerkId -> Version */
}

func (this *Value) getVal() (value string) {
    versions := []Version{}
    for _, vers := range(this.mutates) {
        versions = append(versions, vers...)
    }
    sort.Sort(Versions(versions))
    value = ""
    for _, version := range(versions) {
        value += version.Data
    }
    return
}

func (this *Value) putVal(raftIndex int, clerkId int64, seq uint64, value string) (success bool) {
    old, contains := this.mutates[clerkId]
    if (contains && old[len(old) - 1].Seq >= seq) {
        success = false
        return
    }
    this.mutates[clerkId] = []Version {
        Version {
            RaftIndex   : raftIndex,
            ClerkId     : clerkId,
            Seq         : seq,
            Data        : value,
        },
    }
    success = true
    return
}

func (this *Value) appendVal(raftIndex int, clerkId int64, seq uint64, value string) (success bool) {
    old, contains := this.mutates[clerkId]
    if (!contains) {
        this.putVal(raftIndex, clerkId, seq, value)
        success = true
        return
    }
    if (old[len(old) - 1].Seq >= seq) {
        success = false
        return
    }
    this.mutates[clerkId] = append(old, Version {
        RaftIndex   : raftIndex,
        ClerkId     : clerkId,
        Seq         : seq,
        Data        : value,
    })
    success = true
    return
}

type ShardKV struct {
    terminated          int32
    mu                  sync.Mutex
    me                  int
    seq                 uint64
    rf                  *raft.Raft
    make_end            func(string) *labrpc.ClientEnd
    gid                 int

    sm                  *shardctrler.Clerk                  /* sharder manager */
    config              shardctrler.Config
    fsm                 util.RaftFSM
    data                map[string]Value                    /* key -> value */
}

const (
    OP_GET              = 0
    OP_PUT_APPEND       = 1
    OP_UPDATE_CONFIG      = 2
)

type UpdateConfigArgs struct {
    LeaderConfig        shardctrler.Config
}

type UpdateConfigReply struct {
}

func (this *ShardKV) hasKey(key string) bool {
    shard := key2shard(key)
    return this.config.Shards[shard] == this.gid
}

func (this *ShardKV) refreshConfig() {
    this.mu.Lock()
    defer this.mu.Unlock()

    config := this.sm.Query(-1)
    if (this.config.Equal(config)) {
        return
    }
    args := UpdateConfigArgs {
        LeaderConfig        : config.MakeCopy(),
    }
    this.fsm.Submit(int64(this.me), atomic.AddUint64(&this.seq, 1), OP_UPDATE_CONFIG, args)
}

func (this *ShardKV) updateConfig() {
    for !this.killed() {
        time.Sleep(100 * time.Millisecond)
        this.refreshConfig()
    }
}

//func (this *ShardKV) fetchShard() {
//
//}

func (this *ShardKV) Get(args *GetArgs, reply *GetReply) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (!this.hasKey(args.Key)) {
        reply.RaftErr = util.OK
        reply.Err = ErrWrongGroup
        return
    }
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_GET, *args)
    if (success) {
        *reply = result.(GetReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
}

func (this *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    this.mu.Lock()
    defer this.mu.Unlock()

    if (!this.hasKey(args.Key)) {
        reply.RaftErr = util.OK
        reply.Err = ErrWrongGroup
        return
    }
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_PUT_APPEND, *args)
    if (success) {
        *reply = result.(PutAppendReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (this *ShardKV) Kill() {
    atomic.StoreInt32(&this.terminated, 1)
	this.fsm.Kill()
}

func (this *ShardKV) killed() bool {
    return atomic.LoadInt32(&this.terminated) == 1
}

func (this *ShardKV) doGet(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(GetArgs)
    value, contains := this.data[args.Key]
    if (!contains) {
        reply = GetReply {
            Err         : ErrNoKey,
            Value       : "",
        }
        return
    }
    reply = GetReply {
        Err         : OK,
        Value       : value.getVal(),
    }
    return
}

func (this *ShardKV) doPutAppend(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(PutAppendArgs)
    switch (args.Op) {
    case "Put":
        value, contains := this.data[args.Key]
        if (!contains) {
            value = Value { mutates : make(map[int64]([]Version)) }
            this.data[args.Key] = value
        }
        value.putVal(raftIndex, args.ClerkId, args.ClerkSerial, args.Value)
    case "Append":
        value, contains := this.data[args.Key]
        if (!contains) {
            value = Value { mutates : make(map[int64]([]Version)) }
            this.data[args.Key] = value
        }
        value.appendVal(raftIndex, args.ClerkId, args.ClerkSerial, args.Value)
    default:
        fmt.Printf("op: %v\n", args.Op)
        panic("invalid put-append op\n")
    }
    reply = PutAppendReply { Err : OK }
    return
}

func (this *ShardKV) doUpdateConfig(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(UpdateConfigArgs)
    this.config = args.LeaderConfig
    return UpdateConfigReply{}
}

type Snapshot struct {
    Data            map[string]Value
}

func (this *ShardKV) snapshotSerializer() (snapshotBytes []byte) {
    buf := bytes.Buffer{}
    enc := labgob.NewEncoder(&buf)
    snapshot := Snapshot {
        Data    : this.data,
    }
    enc.Encode(&snapshot)
    snapshotBytes = buf.Bytes()
    return
}

func (this *ShardKV) snapshotDeserializer(snapshotBytes []byte) {
    buf := bytes.NewBuffer(snapshotBytes)
    dec := labgob.NewDecoder(buf)
    snapshot := Snapshot{}
    err := dec.Decode(&snapshot)
    if (err != nil) {
        fmt.Printf("error: %v\n", err)
        panic("decode shardkv snapshot failed\n")
    }
    this.data = snapshot.Data
    return
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd,
                 me int,
                 persister *raft.Persister,
                 maxraftstate int,
                 gid int,
                 ctrlers []*labrpc.ClientEnd,
                 make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(UpdateConfigArgs{})
	labgob.Register(UpdateConfigReply{})

    kv := ShardKV {
        terminated          : int32(0),
        mu                  : sync.Mutex{},
        me                  : me,
        seq                 : uint64(0),
        rf                  : nil,
        make_end            : make_end,
        gid                 : gid,
        sm                  : shardctrler.MakeClerk(ctrlers),
        fsm                 : util.RaftFSM{},
        data                : make(map[string]Value),
    }

    kv.fsm.Init(me, servers, persister, maxraftstate)
    kv.fsm.RegisterHandler(OP_GET, true, kv.doGet)
    kv.fsm.RegisterHandler(OP_PUT_APPEND, false, kv.doPutAppend)
    kv.fsm.RegisterHandler(OP_UPDATE_CONFIG, false, kv.doUpdateConfig)
    kv.fsm.RegisterSnapshotHandler(kv.snapshotSerializer, kv.snapshotDeserializer)
    kv.fsm.Start()

    kv.rf = kv.fsm.Raft()
    kv.refreshConfig()

    go kv.updateConfig()

	return &kv
}
