package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "6.5840/raft"
import "time"
import "crypto/rand"
import "math/big"
import "sync"
import "sync/atomic"
import "reflect"
import "fmt"

type Clerk struct {
    servers         []*labrpc.ClientEnd
    broadcaster     Broadcaster
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
    ck.broadcaster = Broadcaster {
        typeMapping         : make(map[string](reflect.Type)),
    }
    ck.id = nrand()
    ck.serial = uint64(0)

    ck.broadcaster.RegisterPair(JoinArgs{}, JoinReply{})
    ck.broadcaster.RegisterPair(LeaveArgs{}, LeaveReply{})
    ck.broadcaster.RegisterPair(MoveArgs{}, MoveReply{})
    ck.broadcaster.RegisterPair(QueryArgs{}, QueryReply{})
	return ck
}

type Broadcaster struct {
    typeMapping     map[string](reflect.Type)
}

func (this *Broadcaster) RegisterPair(args interface{}, reply interface{}) {
    argsT := reflect.TypeOf(args)
    replyT := reflect.TypeOf(reply)
    _, hasErr := replyT.FieldByName("Err")
    if (!hasErr) {
        fmt.Printf("reply type %v has no 'Err' field\n", replyT.Name())
        panic("unable to register request pair\n")
    }
    this.typeMapping[argsT.Name()] = replyT
}

func (this *Broadcaster) newReplyOf(args interface{}) (replyV reflect.Value) {
    argsV := reflect.Indirect(reflect.ValueOf(args))
    argsT := argsV.Type()
    replyT, contains := this.typeMapping[argsT.Name()]
    if (!contains) {
        fmt.Printf("create reply object of args type %v failed\n", argsT.Name())
        panic("unable to create reply object\n")
    }
    replyV = reflect.New(replyT)
    return
}

func (this *Broadcaster) sendRequest(server *labrpc.ClientEnd, api string,
                                     args interface{}) (success bool, replyV reflect.Value) {
    replyV = this.newReplyOf(args)
    success = server.Call(api, args, replyV.Interface())
    if (!success) {
        return
    }
    errV := reflect.Indirect(replyV).FieldByName("Err")
    success = errV.String() == OK
    return
}

func (this *Broadcaster) Broadcast(servers []*labrpc.ClientEnd, api string,
                                   args interface{}) (success bool, reply interface{}) {
    /* broadcast request asynchronizily */
    replyCh := make(chan reflect.Value, 1)
    wg := sync.WaitGroup{}
    for _, server := range(servers) {
        wg.Add(1)
        go func(server *labrpc.ClientEnd, replyCh chan reflect.Value) {
            success, replyV := this.sendRequest(server, api, args)
            if (success) {
                replyCh <- replyV
            }
            wg.Done()
        } (server, replyCh)
    }
    /* wait for broadcast with timeout */
    done := make(chan bool, 1)
    go func() {
        wg.Wait()
        close(replyCh)
        done <- true
        close(done)
    } ()
    select {
    case <-done:
        success = true
    case <-time.After(time.Duration(2 * raft.HEARTBEAT_TICK_MS) * time.Millisecond):
        success = false
    }
    if (!success) {
        return
    }
    /* fetch result */
    replyV, success := <-replyCh
    if (!success) {
        return
    }
    reply = replyV.Interface()
    return
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
