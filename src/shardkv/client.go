package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "6.5840/shardctrler"
import "6.5840/util"
import "6.5840/raft"

import "crypto/rand"
import "math/big"
import "time"
import "fmt"
import "sync"
import "sync/atomic"
import "log"

func (this *Clerk) debug(format string, a ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[kvclerk %v] ", this.id)
		log.Printf(prefix + format, a...)
	}
	return
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
    sm              *shardctrler.Clerk
    config          shardctrler.Config
    configMu        sync.Mutex
    make_end        func(string) *labrpc.ClientEnd
    // You will have to modify this struct.
    id              int64
    serial          uint64
    broadcaster     util.Broadcaster
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
    ck := Clerk {
        sm              : shardctrler.MakeClerk(ctrlers),
        make_end        : make_end,
        id              : nrand(),
        serial          : uint64(0),
        broadcaster     : util.Broadcaster{},
    }

    ck.config = ck.sm.Query(-1)

    ck.broadcaster.Init()
    ck.broadcaster.RegisterPair(PutAppendArgs{}, PutAppendReply{})
    ck.broadcaster.RegisterPair(GetArgs{}, GetReply{})

	return &ck
}

func (this *Clerk) getServersContainsKey(key string) (configNum int, servers [](*labrpc.ClientEnd)) {
    this.configMu.Lock()
    defer this.configMu.Unlock()

    for {
        gid := this.config.Shards[key2shard(key)]
        servers = this.config.GetServersOf(gid, this.make_end)
        if (len(servers) > 0) {
            break
        }
        time.Sleep(raft.HEARTBEAT_TICK_MS * time.Millisecond)
        this.config = this.sm.Query(-1)
    }
    configNum = this.config.Num
    return
}

func (this *Clerk) refreshConfig() {
    this.configMu.Lock()
    defer this.configMu.Unlock()

    this.config = this.sm.Query(-1)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (this *Clerk) Get(key string) (value string) {
    args := GetArgs {
        ClerkId         : this.id,
        ClerkSerial     : atomic.AddUint64(&this.serial, 1),
        Key             : key,
    }
	for {
        configNum, servers := this.getServersContainsKey(key)
        args.ConfigNum = configNum
        success, data := this.broadcaster.Broadcast(servers, "ShardKV.Get", &args)
        if (!success) {
            this.refreshConfig()
            time.Sleep(raft.HEARTBEAT_TICK_MS * time.Millisecond)
            continue
        }
        reply := data.(*GetReply)
        if (reply.Err == OK) {
            value = reply.Value
            break
        } else if (reply.Err == ErrNoKey) {
            value = ""
            break
        } else if (reply.Err == ErrWrongGroup) {
            time.Sleep(raft.HEARTBEAT_TICK_MS * time.Millisecond)
            this.refreshConfig()
            continue
        } else if (reply.Err == ErrRetryLock) {
            time.Sleep(10 * time.Millisecond) /* todo: should be random */
            continue
        } else {
            fmt.Printf("err %v\n", reply.Err)
            panic("unexpected reply error\n")
        }
	}
    this.debug("GET %v->%v\n", key, value)
	return
}

// shared by Put and Append.
// You will have to modify this function.
func (this *Clerk) PutAppend(key string, value string, op string) (oldVal string) {
    args := PutAppendArgs {
        ClerkId         : this.id,
        ClerkSerial     : atomic.AddUint64(&this.serial, 1),
        Key             : key,
        Value           : value,
        Op              : op,
    }
	for {
        configNum, servers := this.getServersContainsKey(key)
        args.ConfigNum = configNum
        success, data := this.broadcaster.Broadcast(servers, "ShardKV.PutAppend", &args)
        if (!success) {
            this.refreshConfig()
            time.Sleep(raft.HEARTBEAT_TICK_MS * time.Millisecond)
            continue
        }
        reply := data.(*PutAppendReply)
        if (reply.Err == OK) {
            oldVal = reply.OldValue
            break
        } else if (reply.Err == ErrRetryLock) {
            time.Sleep(10 * time.Millisecond) /* todo: should be random */
            continue
        } else if (reply.Err == ErrWrongGroup || reply.Err == ErrStaledConfig) {
            time.Sleep(raft.HEARTBEAT_TICK_MS * time.Millisecond)
            this.refreshConfig()
            args.ClerkSerial = atomic.AddUint64(&this.serial, 1)
            continue
        } else {
            fmt.Printf("err %v\n", reply.Err)
            panic("unexpected reply error\n")
        }
	}
    return
}

func (this *Clerk) Put(key string, value string) (oldVal string) {
    oldVal = this.PutAppend(key, value, "Put")
    this.debug("PUT %v->%v, old value: %v\n", key, value, oldVal)
    return
}

func (this *Clerk) Append(key string, value string) (oldVal string) {
    oldVal = this.PutAppend(key, value, "Append")
    this.debug("APPEND %v->%v, old value: %v\n", key, value, oldVal)
    return
}
