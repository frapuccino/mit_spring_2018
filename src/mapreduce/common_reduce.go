package mapreduce

import (
	"os"
	"encoding/json"
//	"fmt"
	"sort"
	"strings"
)
type ByKey []KeyValue
func (b ByKey) Len() int {
	return len(b)
}
func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b ByKey) Less(i, j int) bool { 
	return (strings.Compare(b[i].Key, b[j].Key) == -1)
}
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task:  
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var allKv []KeyValue
	for i := 0; i < nMap; i++ {
		f, _ := os.OpenFile(reduceName(jobName, i, reduceTask), os.O_RDWR, 0777)
		dec := json.NewDecoder(f)
		var oneFileKv KeyValue
		for {
			if err := dec.Decode(&oneFileKv); err != nil {
				break;
			}
			allKv = append(allKv, oneFileKv)
		}
	}
/*	for _, val := range allKv {
		fmt.Println(val.Value)
	}
*/	
	sort.Sort(ByKey(allKv))
	var retKv []KeyValue
	for i := 0; i < len(allKv); i++ {
		var j int
		curKey := allKv[i].Key
		var tmp []string

		for j = i; (j < len(allKv) && allKv[j].Key == curKey); j++ {
			tmp = append(tmp, allKv[j].Value)
		}
		combVal := reduceF(curKey, tmp)
		retKv = append(retKv, KeyValue{curKey, combVal})
		i = j
		i--
	}
//	fmt.Println(retKv)
//	filename := mergeName(jobName, reduceTask)
	file, _ := os.Create(outFile)
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, kv := range retKv {
		enc.Encode(KeyValue{kv.Key, kv.Value})
	}
}
