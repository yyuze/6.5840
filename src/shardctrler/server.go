package shardctrler

import "6.5840/labrpc"
import "6.5840/labgob"
import "6.5840/raft"
import "6.5840/util"

import "fmt"
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
    me                  int
    rf                  *raft.Raft

    configs             []Config
    fsm                 util.RaftFSM
}

const (
    OP_JOIN             = 0
    OP_LEAVE            = 1
    OP_MOVE             = 2
    OP_QUERY            = 3
)

func (this *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_JOIN, *args)
    if (success) {
        *reply = result.(JoinReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
    return
}

func (this *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_LEAVE, *args)
    if (success) {
        *reply = result.(LeaveReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
}

func (this *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_MOVE, *args)
    if (success) {
        *reply = result.(MoveReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
}

func (this *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
    success, result := this.fsm.Submit(args.ClerkId, args.ClerkSerial, OP_QUERY, *args)
    if (success) {
        *reply = result.(QueryReply)
        reply.RaftErr = util.OK
    } else {
        reply.RaftErr = util.RETRY
    }
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

func (this *ShardCtrler) doJoin(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(JoinArgs)
    config := this.newConfig()
    for gid, servers := range(args.Servers) {
        config.Groups[gid] = servers
    }
    balanceShards(&config)
    this.configs = append(this.configs, config)
    reply = JoinReply{ RaftErr : util.OK }
    return
}

func (this *ShardCtrler) doLeave(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(LeaveArgs)
    config := this.newConfig()
    for _, gid := range(args.GIDs) {
        delete(config.Groups, gid)
    }
    balanceShards(&config)
    this.configs = append(this.configs, config)
    reply = LeaveReply{ RaftErr : util.OK }
    return
}

func (this *ShardCtrler) doMove(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(MoveArgs)
    config := this.newConfig()
    config.Shards[args.Shard] = args.GID
    this.configs = append(this.configs, config)
    reply = MoveReply{ RaftErr : util.OK }
    return
}

func (this *ShardCtrler) doQuery(raftIndex int, data interface{}) (reply interface{}) {
    args := data.(QueryArgs)
    if (args.Num < 0 || args.Num >= len(this.configs)) {
        reply = QueryReply {
            RaftErr         : util.OK,
            Config          : this.configs[len(this.configs) - 1],
        }
        return
    }
    for _, config := range(this.configs) {
        if (config.Num == args.Num) {
            reply = QueryReply {
                RaftErr         : util.OK,
                Config          : config,
            }
            break
        }
    }
    return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (this *ShardCtrler) Kill() {
    this.fsm.Kill()
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
	labgob.Register(util.Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

    sc := ShardCtrler {
        me          : me,
        fsm         : util.RaftFSM{},
        configs     : make([]Config, 1),
    }
	sc.configs[0].Groups = map[int][]string{}

    sc.fsm.Init(me, servers, persister, -1)
    sc.fsm.RegisterHandler(OP_JOIN, false, sc.doJoin)
    sc.fsm.RegisterHandler(OP_LEAVE, false, sc.doLeave)
    sc.fsm.RegisterHandler(OP_MOVE, false, sc.doMove)
    sc.fsm.RegisterHandler(OP_QUERY, true, sc.doQuery)
    sc.fsm.Start()

    sc.rf = sc.fsm.Raft()

	return &sc
}
