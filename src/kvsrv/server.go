package kvsrv

import (
	"log"
	"sync"
    "sort"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Version struct {
    num         int64
    mutation    string
}

type LinearVal struct {
    val         string
    version     map[int64]Version       /* clerk id -> clerk version */
}

type KVServer struct {
	mu          sync.Mutex
    pairs       map[string]LinearVal    /* key -> value */
}

type INT64s []int64

func (arr INT64s) Len() int {
    return len(arr)
}

func (arr INT64s) Swap(i int, j int) {
    temp := arr[j]
    arr[j] = arr[i]
    arr[i] = temp
}

func (arr INT64s) Less(i int, j int) bool {
    return arr[i] < arr[j]
}

func (value *LinearVal) get() (output string) {
    ids := []int64{}
    for id, _ := range value.version {
        ids = append(ids, id)
    }
    sort.Sort(INT64s(ids))
    output = value.val
    for i := 0; i < len(ids); i++ {
        output += value.version[ids[i]].mutation
    }
    return
}

func (value *LinearVal) getUnmutated(clerkId int64) (output string) {
    ids := []int64{}
    for id, _ := range value.version {
        if (id != clerkId) {
            ids = append(ids, id)
        }
    }
    sort.Sort(INT64s(ids))
    output = value.val
    for i := 0; i < len(ids); i++ {
        output += value.version[ids[i]].mutation
    }
    return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    key := args.Key
    clerkId := args.ClerkId
    version := args.Version
    kv.mu.Lock()
    old, contains := kv.pairs[key]
    if (!contains) {
        reply.Value = ""
    } else {
        reply.Value = old.get()
    }
    DPrintf("GET id %v version %v key: %v reply: %v\n", clerkId, version, key, reply.Value)
    kv.mu.Unlock()
}

func (kv *KVServer) putNew(clerkId int64, version int64, key string, val string) {
    newVal := LinearVal {
        val        : "",
        version    : make(map[int64]Version),
    }
    newVal.version[clerkId] = Version { num: version, mutation: val }
    kv.pairs[key] = newVal
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
    key := args.Key
    val := args.Value
    clerkId := args.ClerkId
    version := args.Version
    kv.mu.Lock()
    kv.putNew(clerkId, version, key, val)
    DPrintf("PUT id %v version %v key: %v val: %v\n", clerkId, version, key, val)
    kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
    key := args.Key
    val := args.Value
    clerkId := args.ClerkId
    version := args.Version
    kv.mu.Lock()
    old, contains := kv.pairs[key]
    if (!contains) {
        kv.putNew(clerkId, version, key, val)
        reply.Value = ""
    } else {
        /* check clerk version */
        curVer, contains := old.version[clerkId]
        if (contains && curVer.num < version) {
            /* update version */
            old.val = old.val + curVer.mutation
        }
        /*
         * when a APPEND is delayed on network, but client has retryed and successed,
         * it may cause curVer.num > version, at which case we should not update the version.
         */
        if (!contains || curVer.num < version) {
            old.version[clerkId] = Version{ num: version, mutation: val }
        }
        /*
         * append all mutation values from other clients
         */
        reply.Value = old.getUnmutated(clerkId)
        /* update kv */
        kv.pairs[key] = old
    }
    DPrintf("APPEND id %v version %v key: %v val: %v, reply: %v\n", clerkId, version, key, val, reply.Value)
    kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
    kv.mu = sync.Mutex{}
    kv.pairs = make(map[string]LinearVal)
	return kv
}
