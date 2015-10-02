package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	datasFlags  = datas.NewFlags()
	inputRefStr = flag.String("input-ref", "", "ref to list containing nodes")
	outputDs    = flag.String("output-ds", "", "dataset to store data in.")
	quietFlag   = flag.Bool("quiet", false, "suppress printing of progress statements")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	start := time.Now()

	datastore, ok := datasFlags.CreateDataStore()
	if !ok || *inputRefStr == "" || *outputDs == "" {
		flag.Usage()
		return
	}
	defer datastore.Close()
	dataset := dataset.NewDataset(datastore, *outputDs)

	var inputRef ref.Ref
	err := d.Try(func() {
		inputRef = ref.Parse(*inputRefStr)
	})
	if err != nil {
		log.Fatalf("Invalid inputRef: %v", *inputRefStr)
	}

	gr := GeorectangleDef{GeopositionDef{37.83, -122.52}, GeopositionDef{37.70, -122.36}}
	qtRoot := QuadTreeDef{
		Nodes:          ListOfNodeDef{},
		Tiles:          MapOfStringToQuadTreeDef{},
		Depth:          0,
		NumDescendents: 0,
		Path:           "",
		Georectangle:   gr,
	}
	if !*quietFlag {
		fmt.Printf("quadTreeRoot: %+v\n", qtRoot.Georectangle)
	}

	val := types.ReadValue(inputRef, dataset.Store())
	list := ListOfNodeFromVal(val)
	if !*quietFlag {
		fmt.Printf("Reading from nodeList: %d items, elapsed time: %.2f secs\n", list.Len(), secsSince(start))
	}

	nChan := make(chan *NodeDef, 1024)
	nodesConverted := uint32(0)
	go func() {
		list.l.IterAllP(64, func(v types.Value, i uint64) {
			n := NodeFromVal(v)
			nodeDef := &NodeDef{Geoposition: n.Geoposition().Def(), Reference: n.Ref()}
			nChan <- nodeDef
			nConverted := atomic.AddUint32(&nodesConverted, 1)
			if !*quietFlag && nConverted%1e5 == 0 {
				fmt.Printf("Nodes Converted: %d, elapsed time: %.2f secs\n", nodesConverted, secsSince(start))
			}
		})
		close(nChan)
	}()

	nodesAppended := uint32(0)
	for nodeDef := range nChan {
		qtRoot.Append(nodeDef)
		nodesAppended++
		if !*quietFlag && nodesAppended%1e5 == 0 {
			fmt.Printf("Nodes Appended: %d, elapsed time: %.2f secs\n", nodesAppended, secsSince(start))
			qtRoot.Analyze()
		}
	}

	if !*quietFlag {
		fmt.Printf("Nodes Appended: %d, elapsed time: %.2f secs\n", nodesAppended, secsSince(start))
		qtRoot.Analyze()
		fmt.Printf("Calling SaveToNoms(), elapsed time: %.2f secs\n", secsSince(start))
	}

	nomsQtRoot := qtRoot.SaveToNoms(dataset.Store(), start)
	if !*quietFlag {
		fmt.Printf("Calling Commit(), elapsed time: %.2f secs\n", secsSince(start))
	}
	_, ok = dataset.Commit(nomsQtRoot.NomsValue())
	d.Chk.True(ok, "Could not commit due to conflicting edit")
	if !*quietFlag {
		fmt.Printf("Commit completed, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
	}
	fmt.Println("QuadTree ref:", nomsQtRoot.NomsValue().Ref())
}
