package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "6.5840/raft"
import "6.5840/util"

import "time"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
    servers         []*labrpc.ClientEnd
    broadcaster     util.Broadcaster
    id              int64
    serial          uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
    ck.broadcaster = util.Broadcaster{}
    ck.id = nrand()
    ck.serial = uint64(0)

    ck.broadcaster.Init()
    ck.broadcaster.RegisterPair(JoinArgs{}, JoinReply{})
    ck.broadcaster.RegisterPair(LeaveArgs{}, LeaveReply{})
    ck.broadcaster.RegisterPair(MoveArgs{}, MoveReply{})
    ck.broadcaster.RegisterPair(QueryArgs{}, QueryReply{})
	return ck
}


func (this *Clerk) Join(servers map[int][]string) {
    args := JoinArgs {
        ClerkId         : this.id,
        ClerkSerial     : atomic.AddUint64(&this.serial, 1),
        Servers         : servers,
    }
    for {
        success, _ := this.broadcaster.Broadcast(this.servers, "ShardCtrler.Join", &args)
        if (success) {
            break
        }
        time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
    }
}

func (this *Clerk) Leave(gids []int) {
    args := LeaveArgs {
        ClerkId         : this.id,
        ClerkSerial     : atomic.AddUint64(&this.serial, 1),
        GIDs            : gids,
    }
    for {
        success, _ := this.broadcaster.Broadcast(this.servers, "ShardCtrler.Leave", &args)
        if (success) {
            break
        }
        time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
    }
}

func (this *Clerk) Move(shard int, gid int) {
    args := MoveArgs {
        ClerkId         : this.id,
        ClerkSerial     : atomic.AddUint64(&this.serial, 1),
        Shard           : shard,
        GID             : gid,
    }
    for {
        success, _ := this.broadcaster.Broadcast(this.servers, "ShardCtrler.Move", &args)
        if (success) {
            break
        }
        time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
    }
}

func (this *Clerk) Query(num int) Config {
    args := QueryArgs {
        ClerkId         : this.id,
        ClerkSerial     : atomic.AddUint64(&this.serial, 1),
        Num             : num,
    }
    for {
        success, reply := this.broadcaster.Broadcast(this.servers, "ShardCtrler.Query", &args)
        if (success) {
            return reply.(*QueryReply).Config
        }
        time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
    }
}
