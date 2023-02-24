package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// Inifinite loop to ask for work until the coordinator says it's done.
		reply := CallGetWork()
		if reply.WorkType == 0 {
			break
		}

		// Map
		if reply.WorkType == 1 {
			// Read file
			// Get kva from mapf
			// Split kva into nReduce parts, based on the hash(key).
			// Write the splited kva into json files individually.
			// Call done
			// TODO: What if the coordinator does not reply?
			filename := reply.Filename
			// fmt.Printf("Get map filename: %s\n", filename)
			idx := reply.Idx
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// Split into nReduce files
			kvaMap := make(map[int][]KeyValue)
			for _, kv := range kva {
				i := ihash(kv.Key) % reply.NReduce
				kvaMap[i] = append(kvaMap[i], kv)
			}

			ok := true
			for key, value := range kvaMap {
				jsonData, err := json.MarshalIndent(value, "", "    ")
				if err != nil {
					// fmt.Println(err)
					ok = false
					break
				}

				err = ioutil.WriteFile(fmt.Sprintf("mr-%d-%d.json", idx, key), jsonData, 0644)
				if err != nil {
					// fmt.Println(err)
					ok = false
					break
				}
			}
			if !ok {
				// Something wrong. Drop the work. Ask for works again.
				continue
			}
			CallMapDone(idx)
		} else if reply.WorkType == 2 {
			// Reduce
			// Read all the json file to get kva
			// Sort kva
			// Gather an array of value with the same key of kva and call reducef (just like mrsequential)
			// Record the output of reducef
			// Iterate all kva
			// Write the output to file
			// Call DoneReduce
			idx := reply.Idx
			files, err := filepath.Glob(fmt.Sprintf("mr-*-%d.json", idx))
			if err != nil {
				// fmt.Println(err)
				continue
			}

			var kva []KeyValue
			ok := true
			for _, f := range files {
				jsonData, err := ioutil.ReadFile(f)
				if err != nil {
					// fmt.Println(err)
					ok = false
					break
				}
				var kvaTmp []KeyValue
				err = json.Unmarshal(jsonData, &kvaTmp)
				if err != nil {
					// fmt.Println(err)
					ok = false
					break
				}
				kva = append(kva, kvaTmp...)
			}
			if !ok {
				continue
			}
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%d", idx)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			CallReduceDone(idx)
		} else if reply.WorkType == 3 {
			// Hang
			time.Sleep(time.Second)
		} else {
			// Done
			break
		}
		time.Sleep(time.Second)
	}
	return

}

func CallGetWork() GetWorkReply {
	args := GetWorkArgs{}
	reply := GetWorkReply{}
	call("Coordinator.GetWork", &args, &reply)
	return reply
}

func CallMapDone(idx int) MapDoneReply {
	args := MapDoneArgs{
		Idx: idx,
	}
	reply := MapDoneReply{}
	call("Coordinator.MapDone", &args, &reply)
	return reply
}

func CallReduceDone(idx int) ReduceDoneReply {
	args := ReduceDoneArgs{
		Idx: idx,
	}
	reply := ReduceDoneReply{}
	call("Coordinator.ReduceDone", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	return false
}
