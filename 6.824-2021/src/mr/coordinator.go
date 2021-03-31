package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	_mapState = iota
	_reduceState
	_mergeState
	_finishedState
)

type Coordinator struct {
	// Your definitions here.
	finished bool

	filesTaskChan chan string

	state int

	sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	return c.finished
}

func (c *Coordinator) createFilesTask(files []string) {
	c.filesTaskChan = make(chan string, len(files))
	go func() {
		for _, v := range files {
			c.filesTaskChan <- v
		}
	}()
}

func (c *Coordinator) schedule() {
	for {
		switch c.state {
		case _mapState:
			if len(c.filesTaskChan) == 0 {
				c.nextState()
				continue
			}
		case _reduceState:
		}
	}
}

func (c *Coordinator) nextState() {
	c.Lock()
	defer c.Unlock()
	c.state++
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state: _mapState,
	}
	// Your code here.
	// create files tasks
	c.createFilesTask(files)
	// do map task
	c.schedule()
	c.server()
	return &c
}
