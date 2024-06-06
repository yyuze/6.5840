package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "errors"
import "time"

type Task struct {
    id          int
    input       []string
    output      []TaskOutput
    expireAt    int64
}

type Coordinator struct {
    nReduce     int
    taskType    TaskType
    ready       map[int]Task
    running     map[int]Task
    finished    map[int]Task
    straggler   map[int]Task
    mu          sync.Mutex
    done        *sync.Cond
}

func (this *Task) refreshExpire() {
    /* expires at 1 sec after, at when being marked as a straggler */
    this.expireAt = time.Now().Unix() + 10
}

func (c *Coordinator) FetchTask(args *ArgsFetchTask, reply *ReplyFetchTask) (err error) {
    err = nil

    c.mu.Lock()
    var task Task
    /* fetch task from ready queue */
    for key, val := range c.ready {
        task = val
        task.refreshExpire()
        /* move task from ready queue to running queue */
        delete(c.ready, key)
        c.running[key] = task
        goto end
    }
    /* fetch task from straggler queue (re-execute straggler task) */
    for key, val := range c.straggler {
        task = val
        task.refreshExpire()
        /* move task from straggler queue to running queue */
        delete(c.straggler, key)
        c.running[key] = task
        goto end
    }
    /* all task is done or no staggler task */
    task = Task{ id : -1 }

end:
    reply.Type = c.taskType
    reply.NReduce = c.nReduce
    reply.InputFiles = task.input
    reply.TaskId = task.id
    c.mu.Unlock()
    return
}

func (c *Coordinator) reportTask(taskId int, rpcErr RPCError, output []TaskOutput) {
    /* 1. remove from running queue */
    task, contains := c.running[taskId]
    if (contains) {
        delete(c.running, taskId)
    } else {
        /* remove from straggler queue */
        task, contains = c.straggler[taskId]
        if (contains) {
            delete(c.straggler, taskId)
        } else {
            /* already reported by other worker */
            return
        }
    }
    /* 2. re-execution check */
    re_exec := false
    if (rpcErr.Code != 0) {
        /* check main task status */
        log.Printf("RPC error happend, %v, %v", rpcErr.Code, rpcErr.Reason)
        re_exec = true
    } else {
        /* check sub-tasks for each reduce-bucket */
        for i := range output {
            re_exec = re_exec || (output[i].Code != 0)
            if (output[i].Code != 0) {
                log.Printf("subtask failed, %v-%v, %v, %v", taskId, i, output[i].Code, output[i].Reason)
            }
        }
    }
    /* 3. put to finish queue or running queue */
    if (re_exec) {
        c.ready[taskId] = task
    } else {
        task.output = output
        c.finished[taskId] = task
    }
}

func (c *Coordinator) startReduce() {
    /* 1. init reduce tasks */
    for i := 0; i < c.nReduce; i++ {
        c.ready[i] = Task {
            id          : i,
            input       : []string{},
            expireAt    : -1,
        }
    }
    /* 2. padding output of map subtasks to ready queue */
    for id, task := range c.finished {
        for i := range task.output {
            reduceTask, _ := c.ready[i]
            reduceTask.input = append(reduceTask.input, task.output[i].File)
            c.ready[i] = reduceTask
        }
        delete(c.finished, id)
    }
    /* 3. switch to REDUCE phase */
    c.taskType = TASK_TYPE_REDUCE
}

func (c *Coordinator) finish() {
    for id, task := range c.finished {
        log.Printf("mr[%v] finished: %v", id, task.output[0].File)
    }
    /* switch to TERMINATED phase */
    c.taskType = TASK_TYPE_TERMINATED
}

func (c *Coordinator) ReportTask(args *ArgsReportTask, reply *ReplyReportTask) (err error) {
    err = nil
    c.mu.Lock()

    switch c.taskType {
    case TASK_TYPE_MAP:
        c.reportTask(args.Id, args.Err, args.Output)
        if (len(c.ready) == 0 && len(c.running) == 0 && len(c.straggler) == 0) {
            c.startReduce()
        }
    case TASK_TYPE_REDUCE:
        c.reportTask(args.Id, args.Err, args.Output)
        if (len(c.ready) == 0 && len(c.running) == 0) && len(c.straggler) == 0 {
            c.finish()
        }
    case TASK_TYPE_TERMINATED:
        log.Printf("redundant report\n")
        goto end
    default:
        err = errors.New("unknown report task type\n")
    }
end:
    reply.Type = c.taskType
    c.mu.Unlock()
    return
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
    c.mu.Lock()
    c.done.Wait()
    c.mu.Unlock()
	return true
}

func (c *Coordinator) pollStraggler() {
    terminated := false
    for !terminated {
        time.Sleep(1 * time.Second)
        c.mu.Lock()
        cur := time.Now().Unix()
        for id, task := range c.running {
            if (task.expireAt < cur) {
                /* move task from running queue to staggler queue */
                task.expireAt = -1
                delete(c.running, id)
                c.straggler[id] = task
            }
        }
        terminated = c.taskType == TASK_TYPE_TERMINATED
        if (terminated) {
            /* wake up Done() */
            c.done.Signal()
        }
        c.mu.Unlock()
    }
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
        nReduce     : nReduce,
        taskType    : TASK_TYPE_MAP,
        ready       : make(map[int]Task),
        running     : make(map[int]Task),
        finished    : make(map[int]Task),
        straggler   : make(map[int]Task),
        mu          : sync.Mutex{},
    }
    c.done = sync.NewCond(&c.mu)

    /* init MAP tasks: put files into ready task queue */
    for i := range files {
        c.ready[i] = Task {
            id          : i,
            input       : []string{ files[i] },
            expireAt    : -1,
        }
    }

	c.server()
    go c.pollStraggler()
	return &c
}
