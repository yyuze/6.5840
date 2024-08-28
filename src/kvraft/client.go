package kvraft

import "6.5840/labrpc"
import "6.5840/raft"
import "crypto/rand"
import "math/big"
import "log"
import "time"
import "fmt"
import "sync/atomic"

func (this *Clerk) debug(format string, args ...interface{}) {
	if DEBUG {
        prefix := fmt.Sprintf("[clerk %v] ", this.id)
		log.Printf(prefix + format, args...)
	}
	return
}

type Clerk struct {
    servers     []*labrpc.ClientEnd
    id          int64
    serial      int64
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
    ck.id = nrand()
    ck.serial = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (this *Clerk) Get(key string) (value string) {
    args := GetArgs {
        ClerkId     : this.id,
        Serial      : atomic.AddInt64(&this.serial, 1),
        Key         : key,
    }
    for {
        var reply GetReply
        success := true
        for idx, _ := range(this.servers) {
            reply = GetReply{}
            success = this.servers[idx].Call("KVServer.Get", &args, &reply)
            if (success && reply.Err != ErrWrongLeader) {
                break
            }
        }
        if (!success) {
            this.debug("Get ipc failed\n")
            time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
            continue
        }
        if (reply.Err != OK) {
            if (reply.Err == ErrWrongLeader) {
                time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
                continue
            } else {
                this.debug("Get value of key %v failed, %v\n", key, reply.Err)
                value = ""
                break
            }
        }
        value = reply.Value
        break
    }
	return
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (this *Clerk) PutAppend(key string, value string, op string) {
    args := PutAppendArgs {
        ClerkId     : this.id,
        Serial      : atomic.AddInt64(&this.serial, 1),
        Key         : key,
        Value       : value,
    }
    for {
        var reply PutAppendReply
        success := true
        for idx, _ := range(this.servers) {
            reply = PutAppendReply{}
            success = this.servers[idx].Call("KVServer." + op, &args, &reply)
            if (success && reply.Err != ErrWrongLeader) {
                break
            }
        }
        if (!success) {
            this.debug("%v rpc failed\n", op)
            time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
            continue
        }
        if (reply.Err != OK) {
            if (reply.Err == ErrWrongLeader) {
                time.Sleep(raft.HEARTBEAT_TIMEOUT_MS_BASE * time.Millisecond)
                continue
            } else {
                this.debug("%v with key %v failed, %v\n", op, key, reply.Err)
                break
            }
        }
        break
    }
    return
}

func (this *Clerk) Put(key string, value string) {
    this.debug("Put: %v->%v\n", key, value)
	this.PutAppend(key, value, "Put")
    this.debug("Put: %v->%v done\n", key, value)
}

func (this *Clerk) Append(key string, value string) {
    this.debug("Append: %v->%v\n", key, value)
	this.PutAppend(key, value, "Append")
    this.debug("Append: %v->%v done\n", key, value)
}
