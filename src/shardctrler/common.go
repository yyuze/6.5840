package shardctrler

import "6.5840/labrpc"
//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const DEBUG = false

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (this *Config) Equal(config Config) bool {
    return this.Num == config.Num
}

func (this *Config) MakeCopy() (c Config) {
    c = Config{
        Num         : this.Num,
        Shards      : [NShards]int{},
        Groups      : make(map[int][]string),
    }
    for idx, shard := range(this.Shards) {
        c.Shards[idx] = shard
    }
    for gid, servers := range(this.Groups) {
        c.Groups[gid] = servers
    }
    return
}

func (this *Config) Init() {
    this.Num = 0
    for i := 0; i < NShards; i++ {
        this.Shards[i] = 0
    }
    this.Groups = make(map[int][]string)
}

func (this *Config) GetServersOf(gid int, make_end func(string) *labrpc.ClientEnd) (servers [](*labrpc.ClientEnd)) {
    serverNames, contains := this.Groups[gid]
    servers = [](*labrpc.ClientEnd){}
    if (contains) {
        for _, name := range(serverNames) {
            servers = append(servers, make_end(name))
        }
    }
    return
}

type Err string

type JoinArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
	Servers         map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	RaftErr         Err
}

type LeaveArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
	GIDs            []int
}

type LeaveReply struct {
	RaftErr         Err
}

type MoveArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
	Shard           int
	GID             int
}

type MoveReply struct {
	RaftErr         Err
}

type QueryArgs struct {
    ClerkId         int64
    ClerkSerial     uint64
	Num             int // desired config number
}

type QueryReply struct {
	RaftErr         Err
	Config      Config
}
