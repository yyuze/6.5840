package util

import "6.5840/labrpc"
import "6.5840/raft"

import "time"
import "reflect"
import "fmt"
import "sync"

const (
    OK                  = "OK"
    RETRY               = "retry"
    CONFIG_NOT_FIND     = "config is not find"
)

type Broadcaster struct {
    typeMapping     map[string](reflect.Type)
}

func (this *Broadcaster) Init() {
    this.typeMapping = make(map[string](reflect.Type))
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
