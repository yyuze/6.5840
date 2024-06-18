package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "sync"

type Clerk struct {
	server      *labrpc.ClientEnd
	mu          sync.Mutex
    id          int64
    version     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
    ck.mu = sync.Mutex{}
	ck.server = server
    ck.id = nrand()
    ck.version = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
    ck.mu.Lock()
    args := GetArgs {
        Key         : key,
        ClerkId     : ck.id,
        Version     : atomic.LoadInt64(&ck.version),
    }
    reply := GetReply {}
    ok := false
    for !ok {
        ok = ck.server.Call("KVServer.Get", &args, &reply)
    }
    ck.mu.Unlock()
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
    ck.mu.Lock()
    args := PutAppendArgs {
        Key         : key,
        Value       : value,
        ClerkId     : ck.id,
        Version     : atomic.AddInt64(&ck.version, 1),
    }
    reply := PutAppendReply {}
    ok := false
    for !ok {
        ok = ck.server.Call("KVServer." + op, &args, &reply)
    }
    ck.mu.Unlock()
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
