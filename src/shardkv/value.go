package shardkv

import "sort"
import "time"

type Version struct {
    Timestamp           int64
    RaftIndex           int
    ClerkId             int64
    Seq                 uint64
    Data                string
}

type Versions []Version

func (arr Versions) Len() int {
    return len(arr)
}

func (arr Versions) Swap(i int, j int) {
    temp := arr[j]
    arr[j] = arr[i]
    arr[i] = temp
}

func (arr Versions) Less(i int, j int) bool {
    return arr[i].Timestamp < arr[j].Timestamp
}

type Value struct {
    ConfigNum           int
    Mutates             map[int64]([]Version)                   /* clerkId -> Version */
}

func (this *Value) makeCopy() (value Value) {
    value = Value{
        ConfigNum   : this.ConfigNum,
        Mutates     : make(map[int64]([]Version)),
    }
    for k, v := range(this.Mutates) {
        value.Mutates[k] = v
    }
    return
}

func (this *Value) getVal() (value string) {
    versions := []Version{}
    for _, vers := range(this.Mutates) {
        versions = append(versions, vers...)
    }
    sort.Sort(Versions(versions))
    value = ""
    for _, version := range(versions) {
        value += version.Data
    }
    return
}

func (this *Value) putVal(raftIndex int, clerkId int64, seq uint64, configNum int, value string) (success bool) {
    old, contains := this.Mutates[clerkId]
    if (contains && old[len(old) - 1].Seq >= seq) {
        success = false
        return
    }
    this.Mutates = make(map[int64]([]Version))
    this.Mutates[clerkId] = []Version {
        Version {
            Timestamp   : time.Now().UnixNano(),
            RaftIndex   : raftIndex,
            ClerkId     : clerkId,
            Seq         : seq,
            Data        : value,
        },
    }
    this.ConfigNum = configNum
    success = true
    return
}

func (this *Value) appendVal(raftIndex int, clerkId int64, seq uint64, configNum int, value string) (success bool) {
    old, contains := this.Mutates[clerkId]
    if (!contains) {
        old = []Version{}
    } else {
        if (old[len(old) - 1].Seq >= seq) {
            success = false
            return
        }
    }
    this.Mutates[clerkId] = append(old, Version {
        Timestamp   : time.Now().UnixNano(),
        RaftIndex   : raftIndex,
        ClerkId     : clerkId,
        Seq         : seq,
        Data        : value,
    })
    this.ConfigNum = configNum
    success = true
    return
}
