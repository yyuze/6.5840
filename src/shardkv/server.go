package shardkv

import "6.5840/labgob"
import "6.5840/labrpc"
import "6.5840/raft"
import "6.5840/shardctrler"
import "6.5840/util"

import "math/rand"
import "sync"
import "sync/atomic"
import "bytes"
import "fmt"
import "time"
import "log"

func (this *ShardKV) debug(format string, a ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[shard %v->%v %v] ", this.gid, this.me, this.id)
		log.Printf(prefix + format, a...)
	}
	return
}

type ShardKV struct {
    terminated          int32
    rwmu                sync.RWMutex
    me                  int
    id                  int64                               /* is used for rpc identifier */
    seq                 uint64
    rf                  *raft.Raft
    make_end            func(string) *labrpc.ClientEnd
    gid                 int

    sm                  *shardctrler.Clerk                  /* sharder manager */
    config              shardctrler.Config
    syncConf            FlushCh                             /* a channel to sync config immediately */
    broadcaster         util.Broadcaster
    fsm                 util.RaftFSM

    data                map[string]Value                    /* key -> value */
}

type FlushCh struct {
    mu                  sync.Mutex
    closed              bool
    ch                  chan bool
}

func (this *FlushCh) wait() {
    select {
    case <- time.After(100 * time.Millisecond):
    case <- this.ch:
    }
}

func (this *FlushCh) noti() {
    this.mu.Lock()
    defer this.mu.Unlock()
    if (!this.closed) {
        this.ch <- true
    }
}

func (this *FlushCh) final() {
    this.mu.Lock()
    defer this.mu.Unlock()

    this.ch <- true
    close(this.ch)
    this.closed = true
}

func (this *FlushCh) init() {
    this.mu = sync.Mutex{}
    this.closed = false
    this.ch = make(chan bool, 1)
}

const (
    OP_GET              = 0
    OP_PUT_APPEND       = 1
    OP_FETCH_KVS        = 2
    OP_INSTALL_KVS      = 3
    OP_NOP              = 4         /* for identify leadership of raft fsm */
)

func (this *ShardKV) isLeader() bool {
    success, _ := this.fsm.Submit(this.id, atomic.AddUint64(&this.seq, 1), OP_NOP, NopArgs{})
    return success
}

func (this *ShardKV) fetchKeys(newConfig *shardctrler.Config, configs []shardctrler.Config) (kvs map[string]Value) {
    kvs = make(map[string]Value)
    for _, oldConfig := range(configs) {
        /* fetch kvs from $oldConfig */
        for gid, _ := range(oldConfig.Groups) {
            /* fetch kvs from each shard */
            if (gid == this.gid) {
                continue
            }
            args := FetchKVsArgs {
                ClerkId         : this.id,
                ClerkSerial     : atomic.AddUint64(&this.seq, 1),
                NewConfig       : newConfig.MakeCopy(),
                OldConfig       : oldConfig,
                Gid             : this.gid,
            }
            servers := oldConfig.GetServersOf(gid, this.make_end)
            /* keep retry until successfully fetched kvs */
            for !this.killed() {
                ok, data := this.broadcaster.Broadcast(servers, "ShardKV.FetchKVs", &args)
                if (!ok) {
                    time.Sleep(time.Duration(rand.Int63() % 50) * time.Millisecond)
                    continue
                }
                reply := data.(*FetchKVsReply)
                if (reply.Err != OK) {
                    fmt.Printf("error: %v\n", reply.Err)
                    panic("unexpected error")
                }
                for k, v := range(reply.KVs) {
                    val, contains := kvs[k]
                    if ((contains && val.ConfigNum < v.ConfigNum) || !contains) {
                        kvs[k] = v
                    }
                }
                break
            }
        }
    }
    return
}

func (this *ShardKV) installKVs(kvs map[string]Value, newConfig *shardctrler.Config) (success bool) {
    args := InstallKVsArgs {
        Config          : newConfig.MakeCopy(),
        KVs             : kvs,
    }
    success, _ = this.fsm.Submit(this.id, atomic.AddUint64(&this.seq, 1), OP_INSTALL_KVS, args)
    return
}

func (this *ShardKV) fetchConfigs(latest *shardctrler.Config) (configs []shardctrler.Config) {
    configs = []shardctrler.Config{}
    for i := 1; i < latest.Num; i++ {
        configs = append(configs, this.sm.Query(i))
    }
    return
}

func (this *ShardKV) updateConfig() {
    refreshing := uint32(0)
    for !this.killed() {
        this.syncConf.wait()
        if (!this.isLeader()) {
            continue
        }
        go func() {
            var newConfig   shardctrler.Config
            var configs     []shardctrler.Config
            var kvs         map[string]Value

            if (!atomic.CompareAndSwapUint32(&refreshing, 0, 1)) {
                goto end
            }
            if (this.killed()) {
                goto done_refresh
            }
            /* check config */
            this.rwmu.Lock()
            newConfig = this.sm.Query(-1)
            if (this.config.Num == newConfig.Num) {
                this.rwmu.Unlock()
                goto done_refresh
            }
            this.rwmu.Unlock()
            /* fetch previous configs */
            configs = this.fetchConfigs(&newConfig)
            /* fetch keys belong to this shard from each other groups */
            kvs = this.fetchKeys(&newConfig, configs)
            /* install fetched kvs to fsm */
            this.rwmu.Lock()
            if (!this.installKVs(kvs, &newConfig)) {
                /* fails if &this is not leader */
                goto unlock
            }
            this.config = newConfig

            this.debug("updated config from %v to %v\n", this.config, newConfig)
            /* todo: removed fetched keys here */
        unlock:
            this.rwmu.Unlock()
        done_refresh:
            atomic.StoreUint32(&refreshing, 0)
        end:
            return
        } ()
    }
}

func isKeyInGroup(key string, gid int, config *shardctrler.Config) bool {
    shard := key2shard(key)
    shardGid := config.Shards[shard]
    return shardGid == gid
}

func (this *ShardKV) Get(args *GetArgs, reply *GetReply) {
    if (!this.rwmu.TryRLock()) {
        reply.RaftErr = util.RETRY
        return
    }
    defer this.rwmu.RUnlock()

    /* synchronize configuration */
    config := this.sm.Query(-1)
    if (config.Num != this.config.Num) {
        this.syncConf.noti()
        reply.RaftErr = util.RETRY
        return
    }
    /* check whether the key is in this shard group */
    if (!isKeyInGroup(args.Key, this.gid, &this.config)) {
        reply.RaftErr = util.RETRY
        return
    }
    /* submit raft log */
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_GET, *args)
    if (success) {
        *reply = result.(GetReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
}

func (this *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    if (!this.rwmu.TryRLock()) {
        reply.RaftErr = util.RETRY
        return
    }
    defer this.rwmu.RUnlock()

    /* synchronize configuration */
    config := this.sm.Query(-1)
    if (config.Num != this.config.Num) {
        this.syncConf.noti()
        reply.RaftErr = util.RETRY
        return
    }
    /* check whether the key is in this shard group */
    if (!isKeyInGroup(args.Key, this.gid, &this.config)) {
        reply.RaftErr = util.RETRY
        return
    }
    /* submit raft log */
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_PUT_APPEND, *args)
    if (success) {
        *reply = result.(PutAppendReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
    return
}

func (this *ShardKV) FetchKVs(args *FetchKVsArgs, reply *FetchKVsReply) {
    if (!this.rwmu.TryRLock()) {
        reply.RaftErr = util.RETRY
        return
    }
    defer this.rwmu.RUnlock()

    config := this.sm.Query(-1)
    if (config.Num != this.config.Num) {
        this.syncConf.noti()
    }

    /* submit raft log */
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_FETCH_KVS, *args)
    if (success) {
        *reply = result.(FetchKVsReply)
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
    this.syncConf.final()
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
    this.debug("GET(fsm) %v -> %v\n", args.Key, value.getVal())
    return
}

func (this *ShardKV) doPutAppend(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(PutAppendArgs)
    oldVal := ""
    switch (args.Op) {
    case "Put":
        value, contains := this.data[args.Key]
        if (!contains) {
            oldVal = ""
            value = Value { Mutates : make(map[int64]([]Version)) }
        } else {
            oldVal = value.getVal()
        }
        value.putVal(raftIndex, args.ClerkId, args.ClerkSerial, args.ConfigNum, args.Value)
        this.data[args.Key] = value
        this.debug("PUT(fsm) %v -> %v, added: %v\n", args.Key, value.getVal(), args.Value)
    case "Append":
        value, contains := this.data[args.Key]
        if (!contains) {
            oldVal = ""
            value = Value { Mutates : make(map[int64]([]Version)) }
        } else {
            oldVal = value.getVal()
        }
        if (value.ConfigNum == args.ConfigNum) {
            value.appendVal(raftIndex, args.ClerkId, args.ClerkSerial, args.ConfigNum, args.Value)
            this.data[args.Key] = value
            this.debug("APPEND(fsm) %v -> %v, added %v\n", args.Key, value.getVal(), args.Value)
        } else {
            reply = PutAppendReply { Err : ErrStaledConfig }
            return
        }
    default:
        fmt.Printf("op: %v\n", args.Op)
        panic("invalid put-append op\n")
    }
    reply = PutAppendReply {
        Err         : OK,
        OldValue    : oldVal,
    }
    return
}

func (this *ShardKV) doFetchKVs(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(FetchKVsArgs)
    kvs := make(map[string]Value)
    for k, v := range(this.data) {
        if (isKeyInGroup(k, args.Gid, &args.NewConfig) && v.ConfigNum == args.OldConfig.Num) {
            kvs[k] = v.makeCopy()
            this.debug("FETCH(fsm) by group %v, %v -> %v\n", args.Gid, k, v.getVal())
        }
    }
    reply = FetchKVsReply {
        Err         : OK,
        KVs         : kvs,
    }
    return
}

func (this *ShardKV) doInstallKVs(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(InstallKVsArgs)
    /* put keys into shard */
    for k, v := range(args.KVs) {
        val, contains := this.data[k]
        if ((contains && val.ConfigNum < v.ConfigNum) || !contains) {
            this.data[k] = v
            this.debug("INSTALL(fsm) %v -> %v\n", k, v.getVal())
        }
    }
    /* refresh config number for each key */
    for k, v := range(this.data) {
        if (isKeyInGroup(k, this.gid, &args.Config)) {
            v.ConfigNum = args.Config.Num
            this.data[k] = v
        }
    }
    reply = InstallKVsReply{}
    return
}

func (this *ShardKV) doNop(raftIndex int, data interface{}) (reply interface{}) {
    return NopReply{}
}

type Snapshot struct {
    Data            map[string]Value
}

func (this *ShardKV) snapshotSerializer(snapshotIndex int) (snapshotBytes []byte) {
    buf := bytes.Buffer{}
    enc := labgob.NewEncoder(&buf)
    snapshot := Snapshot {
        Data            : this.data,
    }
    enc.Encode(&snapshot)
    snapshotBytes = buf.Bytes()
    return
}

func (this *ShardKV) snapshotDeserializer(snapshotIndex int, snapshotBytes []byte) {
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
	labgob.Register(FetchKVsArgs{})
	labgob.Register(FetchKVsReply{})
	labgob.Register(InstallKVsArgs{})
	labgob.Register(InstallKVsReply{})
	labgob.Register(NopArgs{})
	labgob.Register(NopReply{})

    kv := ShardKV {
        terminated          : int32(0),
        rwmu                : sync.RWMutex{},
        me                  : me,
        id                  : nrand(),
        seq                 : uint64(0),
        rf                  : nil,
        make_end            : make_end,
        gid                 : gid,
        syncConf            : FlushCh{},
        sm                  : shardctrler.MakeClerk(ctrlers),
        fsm                 : util.RaftFSM{},
        data                : make(map[string]Value),
    }

    kv.broadcaster.Init()
    kv.broadcaster.RegisterPair(FetchKVsArgs{}, FetchKVsReply{})

    kv.syncConf.init()

    kv.fsm.Init(me, servers, persister, maxraftstate)
    kv.fsm.RegisterHandler(OP_GET,              true,       kv.doGet)
    kv.fsm.RegisterHandler(OP_PUT_APPEND,       false,      kv.doPutAppend)
    kv.fsm.RegisterHandler(OP_FETCH_KVS,        true,       kv.doFetchKVs)
    kv.fsm.RegisterHandler(OP_INSTALL_KVS,      false,      kv.doInstallKVs)
    kv.fsm.RegisterHandler(OP_NOP,              true,       kv.doNop)
    kv.fsm.RegisterSnapshotHandler(kv.snapshotSerializer, kv.snapshotDeserializer)
    kv.fsm.Start()

    kv.rf = kv.fsm.Raft()

    go kv.updateConfig()

	return &kv
}
