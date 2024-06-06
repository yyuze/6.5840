package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "fmt"

// Add your RPC definitions here.
type TaskType uint32

const (
    TASK_TYPE_MAP           = 0
    TASK_TYPE_REDUCE        = 1
    TASK_TYPE_TERMINATED    = 2
)

type ArgsFetchTask struct {
}

type ReplyFetchTask struct {
    Type        TaskType
    NReduce     int
    InputFiles  []string
    TaskId      int
}

type RPCError struct {
    Code        int
    Reason      string
}

func (e *RPCError) Error() string {
    return fmt.Sprintf("RPC error, %d, %s", e.Code, e.Reason)
}

type TaskOutput struct {
    Code        int
    Reason      string
    File        string
}

type ArgsReportTask struct {
    Id          int
    Err         RPCError
    Output      []TaskOutput
}

type ReplyReportTask struct {
    Type TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
