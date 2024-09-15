package shardctrler

import "6.5840/raft"
import "6.5840/labrpc"
import "6.5840/labgob"
import "6.5840/util"

import "sync"
import "sync/atomic"
import "fmt"
import "bytes"
import "time"
import "log"
import "sort"

func (this *ShardCtrler) debug(format string, a ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[server %v] ", this.me)
		log.Printf(prefix + format, a...)
	}
	return
}

type ShardCtrler struct {
    mu                  sync.Mutex
    me                  int
    rf                  *raft.Raft
    applyCh             chan raft.ApplyMsg

    configs             []Config
    maxSerial           map[int64]uint64
    opSerial            uint64
    committing          map[uint64](*chan OpResult)
}

const (
    OP_JOIN             = 0
    OP_LEAVE            = 1
    OP_MOVE             = 2
    OP_QUERY            = 3
)

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

func (this *ShardCtrler) raftReplicate(clerkId int64, clerkSerial uint64,
                                        opType uint32, body interface{}) (success bool, result interface{}) {
    /* commit op to committing map */
    resultCh := make(chan OpResult, 1)
    op := Op {
        StartServer     : this.me,
        Serial          : atomic.AddUint64(&this.opSerial, 1),
        ClerkId         : clerkId,
        ClerkSerial     : clerkSerial,
        Type            : opType,
        Body            : body,
    }
    this.mu.Lock()
    this.committing[op.Serial] = &resultCh
    this.mu.Unlock()
    /* start op on raft FSM */
    index, term, success := this.rf.Start(op.toBytes())
    if (!success) {
        this.debug("%v is not leader\n", this.me)
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

func (this *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
    success, result := this.raftReplicate(args.ClerkId, args.ClerkSerial, OP_JOIN, *args)
    if (success) {
        *reply = result.(JoinReply)
        reply.Err = util.OK
    } else {
        reply.Err = util.RETRY
    }
    return
}

func (this *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
    success, result := this.raftReplicate(args.ClerkId, args.ClerkSerial, OP_LEAVE, *args)
    if (success) {
        *reply = result.(LeaveReply)
        reply.Err = util.OK
    } else {
        reply.Err = util.RETRY
    }
}

func (this *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
    success, result := this.raftReplicate(args.ClerkId, args.ClerkSerial, OP_MOVE, *args)
    if (success) {
        *reply = result.(MoveReply)
        reply.Err = util.OK
    } else {
        reply.Err = util.RETRY
    }
}

func (this *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
    success, result := this.raftReplicate(args.ClerkId, args.ClerkSerial, OP_QUERY, *args)
    if (success) {
        *reply = result.(QueryReply)
        reply.Err = util.OK
    } else {
        reply.Err = util.RETRY
    }
}

func (this *ShardCtrler) opProcessed(op Op) (processed bool) {
    maxSerial, contains := this.maxSerial[op.ClerkId]
    if (contains && maxSerial >= op.ClerkSerial) {
        processed = true
    } else {
        this.maxSerial[op.ClerkId] = op.ClerkSerial
        processed = false
    }
    return
}

func (this *ShardCtrler) newConfig() (result Config) {
    epoch := len(this.configs)
    result = Config {
        Num         : epoch,
        Shards      : [NShards]int{},
        Groups      : make(map[int]([]string)),
    }
    oldConfig := this.configs[epoch - 1]
    for gid, servers := range(oldConfig.Groups) {
        result.Groups[gid] = servers
    }
    for shard, gid := range(oldConfig.Shards) {
        result.Shards[shard] = gid
    }
    return
}

type GIDs []int

func (arr GIDs) Len() int {
    return len(arr)
}

func (arr GIDs) Swap(i int, j int) {
    temp := arr[j]
    arr[j] = arr[i]
    arr[i] = temp
}

func (arr GIDs) Less(i int, j int) bool {
    return arr[i] < arr[j]
}

func balanceShards(config *Config) {
    if (len(config.Groups) == 0) {
        return
    }
    /* statistic gid -> { shard1, shrd2... } */
    stat := make(map[int]([]int))
    for i, gid := range(config.Shards) {
        old, contains := stat[gid]
        if (contains) {
            stat[gid] = append(old, i)
        } else {
            stat[gid] = []int{ i }
        }
    }
    /* move shards which should be moved to other gid to 'moving' array */
    statGids := []int{}
    for gid, _ := range(stat) {
        statGids = append(statGids, gid)
    }
    sort.Sort(GIDs(statGids))
    moving := []int{}
    average := NShards / len(config.Groups)
    for _, gid := range(statGids) {
        shards, _ := stat[gid]
        _, contains := config.Groups[gid]
        if (!contains) {
            moving = append(moving, shards...)
            delete(stat, gid)
        } else {
            if (len(shards) > average) {
                moving = append(moving, shards[average : len(shards)]...)
                stat[gid] = shards[0 : average]
            }
        }
    }
    /* move shards in moving array to each gid */
    gids := []int{}
    for gid, _ := range(config.Groups) {
        gids = append(gids, gid)
    }
    sort.Sort(GIDs(gids))
    for _, shard := range(moving) {
        for _, gid := range(gids) {
            shards, contains := stat[gid]
            if (!contains) {
                shards = []int{}
                stat[gid] = shards
            }
            if (len(shards) < average) {
                stat[gid] = append(shards, shard)
                break
            }
        }
    }
    for _, gid := range(gids) {
        shards, _ := stat[gid]
        for _, shard := range(shards) {
            config.Shards[shard] = gid
        }
    }
}

func (this *ShardCtrler) doJoin(args JoinArgs) (reply JoinReply) {
    config := this.newConfig()
    for gid, servers := range(args.Servers) {
        config.Groups[gid] = servers
    }
    balanceShards(&config)
    this.configs = append(this.configs, config)
    reply = JoinReply{ Err : util.OK }
    return
}

func (this *ShardCtrler) doLeave(args LeaveArgs) (reply LeaveReply) {
    config := this.newConfig()
    for _, gid := range(args.GIDs) {
        delete(config.Groups, gid)
    }
    balanceShards(&config)
    this.configs = append(this.configs, config)
    reply = LeaveReply{ Err : util.OK }
    return
}

func (this *ShardCtrler) doMove(args MoveArgs) (reply MoveReply) {
    config := this.newConfig()
    config.Shards[args.Shard] = args.GID
    this.configs = append(this.configs, config)
    reply = MoveReply{ Err : util.OK }
    return
}

func (this *ShardCtrler) doQuery(args QueryArgs) (reply QueryReply) {
    if (args.Num < 0 || args.Num >= len(this.configs)) {
        reply = QueryReply {
            Err         : util.OK,
            Config      : this.configs[len(this.configs) - 1],
        }
        return
    }
    for _, config := range(this.configs) {
        if (config.Num == args.Num) {
            reply = QueryReply {
                Err         : util.OK,
                Config      : config,
            }
            break
        }
    }
    return
}

func (this *ShardCtrler) handleRaftCommand(index int, command interface{}) {
    op := Op {}
    op.fromBytes(command.([]byte))

    this.mu.Lock()
    result := OpResult{}
    processed := this.opProcessed(op)
    switch (op.Type) {
    case OP_JOIN:
        if (!processed) {
            result.body = this.doJoin(op.Body.(JoinArgs))
        }
    case OP_LEAVE:
        if (!processed) {
            result.body = this.doLeave(op.Body.(LeaveArgs))
        }
    case OP_MOVE:
        if (!processed) {
            result.body = this.doMove(op.Body.(MoveArgs))
        }
    case OP_QUERY:
        result.body = this.doQuery(op.Body.(QueryArgs))
    default:
        fmt.Printf("op.Type: %v\n", op.Type)
        panic("unknown operation type\n")
    }
    /* wakeup waiting */
    if (op.StartServer == this.me) {
        ch, waiting := this.committing[op.Serial]
        /* maybe raft-start-timeout before send result to channel */
        if (waiting) {
            (*ch) <- result
        }
    }
    this.mu.Unlock()
}

func (this *ShardCtrler) handleRaft() {
    for msg := range(this.applyCh) {
        if (msg.CommandValid) {
            this.handleRaftCommand(msg.CommandIndex, msg.Command)
        } else if (msg.SnapshotValid) {
            panic("snapshot is not supported\n")
        } else {
            panic("unknwon raft message type\n")
        }
    }
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (this *ShardCtrler) Kill() {
    this.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	applyCh := make(chan raft.ApplyMsg, 1)
    sc := ShardCtrler {
        mu          : sync.Mutex{},
        me          : me,
        rf          : raft.Make(servers, me, persister, applyCh),
        applyCh     : applyCh,
        configs     : make([]Config, 1),
        maxSerial   : make(map[int64]uint64),
        opSerial    : uint64(0),
        committing  : make(map[uint64](*chan OpResult)),
    }
	sc.configs[0].Groups = map[int][]string{}

    go sc.handleRaft()

	return &sc
}
