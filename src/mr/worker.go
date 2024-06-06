package mr

import "os"
import "errors"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "io/ioutil"
import "encoding/json"
import "path/filepath"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

/* for sort */
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readAll(filePath string) (content string, err error) {
    err = nil
    var bytes []byte
    file, err := os.Open(filePath)
    if err != nil {
        log.Printf("cannot open %v", filePath)
        goto end
    }
    bytes, err = ioutil.ReadAll(file)
    if err != nil {
        log.Printf("cannot read %v", filePath)
        goto close_file
    }
    content = string(bytes)

close_file:
    file.Close()
end:
    return content, err
}

func pathExist(path string) (ret bool, err error) {
    err = nil
    _, err = os.Stat(path)
    if (err != nil) {
        if (os.IsNotExist(err)) {
            ret = false
            err = nil
        } else {
            log.Printf("get stat of %v failed, %v", path, err)
        }
    } else {
        ret = true
    }
    return
}

type MapSubTask struct {
    skipped bool
    outPath string
    temp *os.File
    tempPath string
    tempEnc *json.Encoder
}

func (this *MapSubTask) skip() (shouldSkip bool, err error) {
    err = nil
    var exist bool
    if (this.skipped) {
        shouldSkip = this.skipped
        goto end;
    }
    exist, err = pathExist(this.outPath)
    if (err != nil) {
        log.Printf("check output path failed, %v, %v", this.outPath, err)
        goto end;
    }
    shouldSkip = exist
    this.skipped = shouldSkip

end:
    return
}

func (this *MapSubTask) init(mapId int, reduceId int) (err error) {
    err = nil
    var skip bool
    var temp *os.File
    /* get output abs path */
    this.skipped = false
    this.outPath, err = filepath.Abs(fmt.Sprintf("mr-%v-%v", mapId, reduceId))
    if (err != nil) {
        log.Printf("get map output abs path failed, %v", err)
        goto end;
    }
    /* check whether skip or not */
    skip, err = this.skip()
    if (err != nil) {
        log.Printf("get skip state failed, %v", err)
        goto end;
    }
    if (skip) {
        this.temp = nil
        this.tempEnc = nil
        goto end;
    }
    /* create temp output file */
    temp, err = os.CreateTemp("", "*")
    if (err != nil) {
        log.Printf("create temp file failed, %v", err)
        goto end;
    }
    this.temp = temp
    this.tempPath = temp.Name()
    this.tempEnc = json.NewEncoder(this.temp)
    goto end;

end:
    return
}

func (this *MapSubTask) final(force bool) (output string, err error) {
    err = nil
    output = this.outPath
    this.temp.Close()
    this.temp = nil
    if (this.skipped || force) {
        os.Remove(this.tempPath)
        goto end
    }
    err = os.Rename(this.tempPath, this.outPath)
    if (err != nil) {
        log.Printf("rename map temp file to output file failed, %v", err)
        goto end
    }

end:
    return
}

func (this *MapSubTask) persist(kvs []KeyValue) (err error) {
    err = nil
    var skip bool
    skip, err = this.skip()
    if (err != nil) {
        log.Printf("get skip stat failed, %v", err)
        goto end;
    }
    if (skip) {
        goto end;
    }
    /* json encode */
    err = this.tempEnc.Encode(kvs)
    if (err != nil) {
        log.Printf("write inter KVs into json format failed, %v", err)
        goto end
    }

end:
    return
}

func doMap(fn func(string, string) []KeyValue,
           inf string, taskId int, nReduce int) (ofs []string, errs []error, err error) {
    ofs = []string{}
    errs = []error{}
    err = nil

    var content string
    var inter []KeyValue
    /* 1. init sub mapping tasks, each corresponding to a reduce-bucket */
    tasks := make([]MapSubTask, 0, nReduce)
    taskNr := 0
    for ; taskNr < nReduce; taskNr++ {
        task := MapSubTask{}
        err = task.init(taskId, taskNr)
        if (err != nil) {
            log.Printf("init task %v-%v failed, %v", taskId, taskNr, err)
            goto final_tasks
        }
        tasks = append(tasks, task)
    }
    /* 2. read all from input file */
    content, err = readAll(inf)
    if (err != nil) {
        log.Printf("read from input file failed, %v, %v", inf, err)
        goto end;
    }
    /* 3. call mapping function and sort by key */
    inter = fn(inf, content)
	sort.Sort(ByKey(inter))
    /* 4. write result to temp file buckets */
    for i := 0; i < len(inter); {
        key := inter[i].Key
        j := i + 1
        for j < len(inter) && inter[j].Key == key {
            j++;
        }
        /* write inter[i:j] to output bucket file */
        err = tasks[ihash(key) % nReduce].persist(inter[i:j])
        if (err != nil) {
            log.Printf("persist mapping result failed, key %v, %v", key, err)
            goto final_tasks
        }
        i = j
    }
    /* 5. final tasks */
    for i := 0; i < taskNr; i++ {
        task := tasks[i]
        of, err := task.final(false)
        if (err != nil) {
            log.Printf("final task failed, %v, %v", task, err)
            ofs = append(ofs, "")
        } else {
            ofs = append(ofs, of)
        }
        errs = append(errs, err)
        err = nil
    }
    goto end

final_tasks:
    for i := 0; i < taskNr; i++ {
        tasks[i].final(true)
    }
end:
    return
}

func decodeInterKVs(interPath string) (kvs []KeyValue, err error) {
    kvs = []KeyValue{}
    err = nil
    var dec *json.Decoder
    file, err := os.Open(interPath)
    if (err != nil) {
        log.Printf("open intermediate file failed, %v, %v", interPath, err)
        goto end
    }
    dec = json.NewDecoder(file)
    for dec.More() {
        arr := []KeyValue{}
        err = dec.Decode(&arr)
        if (err != nil) {
            log.Printf("decode file failed, %v, %v", interPath, err)
            goto close_file
        }
        kvs = append(kvs, arr...)
    }

close_file:
    file.Close()
end:
    return
}

func doReduce(fn func(string, []string) string,
              inf []string, taskId int) (of string, err error) {
    err = nil

    var skip bool
    var temp *os.File
    var tempPath string
    kvs := []KeyValue{}
    /* 1. check whether task is finished or not */
    reducePath, err := filepath.Abs(fmt.Sprintf("mr-out-%v", taskId))
    if (err != nil) {
        log.Printf("get reduce output path failed, %v", err)
        goto end
    }
    skip, err = pathExist(reducePath)
    if (err != nil) {
        log.Printf("check reduce task failed, %v", err)
        goto end
    }
    if (skip) {
        of = ""
        goto end
    }
    /* 2. read all content and append to kvs */
    for i := range inf {
        arr, err := decodeInterKVs(inf[i])
        if (err != nil) {
            log.Printf("decode inter kvs failed")
            goto end
        }
        kvs = append(kvs, arr...)
    }
	sort.Sort(ByKey(kvs))
    /* 3. create temp file */
    temp, err = os.CreateTemp("", "*")
    if (err != nil) {
        log.Printf("create temp file failed, %v", err)
        goto end
    }
    tempPath = temp.Name()
    /* 4. invoke reduce function for each key and write to temp file */
    for i := 0; i < len(kvs); {
        /* optimize: if task is done by other worker, skip */
        skip, err = pathExist(reducePath)
        if (err != nil) {
            log.Printf("check reduce task failed, %v", err)
            goto rm_temp
        }
        if (skip) {
            of = ""
            goto rm_temp
        }
        key := kvs[i].Key
        j := i + 1
        vals := []string{}
        for j < len(kvs) && kvs[j].Key == key {
            j++
        }
        for k := i; k < j; k++ {
            vals = append(vals, kvs[k].Value)
        }
        reduced := fn(key, vals)
        _, err = fmt.Fprintf(temp, "%v %v\n", key, reduced)
        if (err != nil) {
            log.Printf("write reduce result failed, %v", err)
            goto rm_temp
        }
        i = j
    }
    /* 5. rename temp file to reduce file */
    err = os.Rename(tempPath, reducePath)
    if (err != nil) {
        log.Printf("rename reduce temp file to output file failed, %v", err)
        goto rm_temp
    }
    temp.Close()
    of = reducePath
    goto end;

rm_temp:
    temp.Close()
    temp = nil
    os.Remove(tempPath)
end:
    return
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf    func(string, string) []KeyValue,
            reducef func(string, []string) string) {
	// Your worker implementation here.
    var output []string
    var errs []error
    for {
        /* 1. fetch task from coordernator */
        taskType, inf, taskId, nReduce, err := fetchTask()
        if (err != nil) {
            log.Printf("fetch task failed, %v", err)
            goto err_end
        }
        if (taskType != TASK_TYPE_TERMINATED && taskId == -1) {
            /* all task is now running, sleep then try again */
            time.Sleep(1 * time.Second)
            continue
        }
        /* 2. execute task */
        switch taskType {
        case TASK_TYPE_MAP:
            output, errs, err = doMap(mapf, inf[0], taskId, nReduce)
        case TASK_TYPE_REDUCE:
            of, err := doReduce(reducef, inf, taskId)
            output = []string{ of }
            errs = []error{ err }
        case TASK_TYPE_TERMINATED:
            log.Printf("all task finished")
            goto end
        default:
            output = []string{}
            errs = []error{}
            err = errors.New("unknown task type")
        }
        /* 3. report task executing to coordernator */
        err = reportTask(taskId, output, errs, err)
        if (err != nil) {
            log.Printf("report task failed, %v", err)
            goto err_end;
        }
    }

err_end:
    log.Printf("unexpected worker exit")
end:
    return
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

var IPC_CALL_ERROR error = errors.New("IPC failed")

func fetchTask() (taskType TaskType, inf []string, taskId int, nReduce int, err error) {
    err = nil
    fetchArgs := ArgsFetchTask{}
    fetchReply := ReplyFetchTask{}
    if (!call("Coordinator.FetchTask", &fetchArgs, &fetchReply)) {
        log.Printf("IPC FetchTask failed")
        err = IPC_CALL_ERROR
        goto end
    }
    taskType = fetchReply.Type
    inf = fetchReply.InputFiles
    taskId = fetchReply.TaskId
    nReduce = fetchReply.NReduce

end:
    return
}

func reportTask(taskId int, of []string, errs []error, taskErr error) (err error) {
    err = nil
    reportArgs := ArgsReportTask{ Id : taskId }
    reportReply := ReplyReportTask{}
    if (taskErr != nil) {
        reportArgs.Err.Code = -1
        reportArgs.Err.Reason = taskErr.Error()
    } else {
        reportArgs.Err.Code = 0
        reportArgs.Err.Reason = ""
        for i := range of {
            task := TaskOutput{}
            if (errs[i] != nil) {
                task.Code = -1
                task.Reason = errs[i].Error()
            } else {
                task.Code = 0
                task.Reason = ""
            }
            task.File = of[i]
            reportArgs.Output = append(reportArgs.Output, task)
        }
    }
    if (!call("Coordinator.ReportTask", &reportArgs, &reportReply)) {
        log.Printf("IPC ReportTask failed")
        err = IPC_CALL_ERROR
        goto end
    }

end:
    return
}
