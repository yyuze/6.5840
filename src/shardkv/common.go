package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

import "6.5840/shardctrler"

const DEBUG = false

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
    ErrRetryLock        = "ErrRetryLock"
    ErrStaledConfig     = "ErrStaledConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
    ConfigNum       int
    Key             string
    Value           string
    Op              string // "Put" or "Append"
}

type PutAppendReply struct {
    RaftErr         string
	Err             Err
    OldValue        string
}

type GetArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
    ConfigNum       int
    Key             string
}

type GetReply struct {
    RaftErr         string
	Err             Err
	Value           string
}

type InstallKVsArgs struct {
    Config          shardctrler.Config
    KVs             map[string]Value
}

type InstallKVsReply struct {

}

type FetchKVsArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
    NewConfig       shardctrler.Config
    OldConfig       shardctrler.Config
    Gid             int
}

type FetchKVsReply struct {
    RaftErr         string
    Err             Err
    KVs             map[string]Value
}

type NopArgs struct {

}

type NopReply struct {

}
