package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	idx       int
	state     int // 0: Not dispatched, 1: Dispatched, 2: Done
	timestamp time.Time
	mu        sync.Mutex
}

func (f *Task) Undispatch() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == 1 {
		f.state = 0
		return nil
	} else if f.state == 0 {
		return errors.New("Undispatch failed: status 0")
	} else {
		return errors.New("Undispatch failed: status 2")
	}
}

func (f *Task) Dispatch() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == 0 {
		f.state = 1
		f.timestamp = time.Now()
		return nil
	} else if f.state == 1 {
		return errors.New("Dispatch failed: status 1")
	} else {
		return errors.New("Dispatch failed: status 2")
	}
}

func (f *Task) Done() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == 1 {
		f.state = 2
		return nil
	} else if f.state == 0 {
		return errors.New("Done failed: status 0")
	} else {
		return errors.New("Done failed: status 2")
	}
}

type Counter struct {
	value int
	mu    sync.Mutex
}

func (c *Counter) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
}

func (c *Counter) Decrement() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value--
}

func (c *Counter) GetValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

type Coordinator struct {
	// Your definitions here.
	files             []string
	mapTasks          []Task
	reduceTasks       []Task
	nMap              int
	nReduce           int
	mapDispatchCnt    Counter
	mapDoneCnt        Counter
	reduceDispatchCnt Counter
	reduceDoneCnt     Counter
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	// If not all map are dispatched, dispatch map
	// If all maps are dispatched but not done, wait
	// If all maps are done but not all reduces are dispatch, dispatch reduce
	// If all reduce are dispatched but not done, wait
	// If all reduce are done. Done.
	// TODO: Don't use too many locks.
	// fmt.Println("<<<< Coordinator get a request!")
	if c.mapDispatchCnt.value < c.nMap {
		// dispatch map
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].state == 0 {
				err := c.mapTasks[i].Dispatch()
				if err == nil {
					// fmt.Printf("---> Dispatch Map %d\n", i)
					reply.Filename = c.files[i]
					reply.WorkType = 1
					reply.NReduce = c.nReduce
					reply.Idx = i
					c.mapDispatchCnt.Increment()
					return nil
				}
			}
		}
	} else if c.mapDoneCnt.value < c.nMap {
		// tell worker to wait
		// fmt.Printf("Wait Map...\n")
		reply.WorkType = 3
		return nil
	} else if c.reduceDispatchCnt.value < c.nReduce {
		// dispatch reduce
		// fmt.Printf("Dispatch Reduce...\n")
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].state == 0 {
				err := c.reduceTasks[i].Dispatch()
				if err == nil {
					// fmt.Printf("---> Dispatch Reduce %d\n", i)
					reply.WorkType = 2
					reply.Idx = i
					c.reduceDispatchCnt.Increment()
					return nil
				}
			}
		}
	} else if c.reduceDoneCnt.value < c.nReduce {
		// tell workder to wait
		// fmt.Printf("Wait Reduce...\n")
		reply.WorkType = 3
		return nil
	} else {
		// tell worker everything is done
		// fmt.Printf("Done...\n")
		reply.WorkType = 0
		return nil
	}

	reply.WorkType = 3
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	err := c.mapTasks[args.Idx].Done()
	if err == nil {
		c.mapDoneCnt.Increment()
		// fmt.Printf("Map done %d\n", args.Idx)
		return nil
	}
	// fmt.Println(err)
	return err
}

func (c *Coordinator) ReduceDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	err := c.reduceTasks[args.Idx].Done()
	if err == nil {
		c.reduceDoneCnt.Increment()
		// fmt.Printf("Reduce done %d\n", args.Idx)
		return nil
	}
	// fmt.Println(err)
	return err
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// Check if all reduce jobs are done.
	if c.reduceDoneCnt.value == c.nReduce {
		ret = true
	}

	return ret
}

func BadWorkerChecker(c *Coordinator) {
	for {
		// fmt.Println("Checking bad workers...")
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].state == 1 && time.Now().After(c.mapTasks[i].timestamp.Add(time.Second*10)) {
				// fmt.Println("Got bad map workers...")
				err := c.mapTasks[i].Undispatch()
				if err == nil {
					c.mapDispatchCnt.Decrement()
				}
			}
		}
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].state == 1 && time.Now().After(c.reduceTasks[i].timestamp.Add(time.Second*10)) {
				// fmt.Println("Got bad reduce workers...")
				err := c.reduceTasks[i].Undispatch()
				if err == nil {
					c.reduceDispatchCnt.Decrement()
				}
			}
		}
		time.Sleep(time.Second * 1)
	}
}

func PrintProgress(c *Coordinator) {
	for {
		fmt.Println("-------------------")
		fmt.Printf("%d %d\n", c.mapDoneCnt.value, c.reduceDoneCnt.value)
		for i := 0; i < len(c.mapTasks); i++ {
			fmt.Printf("%d ", c.mapTasks[i].state)
		}
		fmt.Printf("\n")
		for i := 0; i < len(c.reduceTasks); i++ {
			fmt.Printf("%d ", c.reduceTasks[i].state)
		}
		fmt.Println("\n-------------------")
		time.Sleep(time.Second * 5)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapTasks = []Task{}
	for i, f := range files {
		c.files = append(c.files, f)
		c.mapTasks = append(c.mapTasks, Task{idx: i})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{idx: i})
	}

	go BadWorkerChecker(&c)
	// go PrintProgress(&c)

	c.server()
	return &c
}
