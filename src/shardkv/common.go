package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const DEBUG = true

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
    Key             string
    Value           string
    Op              string // "Put" or "Append"
}

type PutAppendReply struct {
    RaftErr         string
	Err             Err
}

type GetArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
    Key             string
}

type GetReply struct {
    RaftErr         string
	Err             Err
	Value           string
}
