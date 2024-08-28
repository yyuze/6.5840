package kvraft

const DEBUG = false

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    ClerkId     int64
    Serial      int64
    Key         string
    Value       string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
    ClerkId     int64
    Serial      int64
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
