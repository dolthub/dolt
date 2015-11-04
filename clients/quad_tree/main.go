package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/attic-labs/noms/clients/common"
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

	gr := common.GeorectangleDef{
		TopLeft:     common.GeopositionDef{Latitude: 37.83, Longitude: -122.52},
		BottomRight: common.GeopositionDef{Latitude: 37.70, Longitude: -122.36},
	}
	qtRoot := common.QuadTreeDef{
		Nodes:          common.ListOfNodeDef{},
		Tiles:          common.MapOfStringToQuadTreeDef{},
		Depth:          0,
		NumDescendents: 0,
		Path:           "",
		Georectangle:   gr,
	}
	if !*quietFlag {
		fmt.Printf("quadTreeRoot: %+v\n", qtRoot.Georectangle)
	}

	list := types.ReadValue(inputRef, dataset.Store()).(types.List)
	if !*quietFlag {
		fmt.Printf("Reading from nodeList: %d items, elapsed time: %.2f secs\n", list.Len(), SecsSince(start))
	}

	nChan := make(chan *common.NodeDef, 1024)
	nodesConverted := uint32(0)
	go func() {
		list.IterAllP(64, func(v types.Value, i uint64) {
			// Need to replace incident with generic type
			r := v.(common.RefOfValue)
			incident := r.TargetValue(datastore).(common.Incident)
			nodeDef := &common.NodeDef{Geoposition: incident.Geoposition().Def(), Reference: r.TargetRef()}
			nChan <- nodeDef
			nConverted := atomic.AddUint32(&nodesConverted, 1)
			if !*quietFlag && nConverted%1e5 == 0 {
				fmt.Printf("Nodes Converted: %d, elapsed time: %.2f secs\n", nodesConverted, SecsSince(start))
			}
		})
		close(nChan)
	}()

	nodesAppended := uint32(0)
	for nodeDef := range nChan {
		qtRoot.Append(nodeDef)
		nodesAppended++
		if !*quietFlag && nodesAppended%1e5 == 0 {
			fmt.Printf("Nodes Appended: %d, elapsed time: %.2f secs\n", nodesAppended, SecsSince(start))
			qtRoot.Analyze()
		}
	}

	if !*quietFlag {
		fmt.Printf("Nodes Appended: %d, elapsed time: %.2f secs\n", nodesAppended, SecsSince(start))
		qtRoot.Analyze()
		fmt.Printf("Calling SaveToNoms(), elapsed time: %.2f secs\n", SecsSince(start))
	}

	nomsQtRoot := qtRoot.SaveToNoms(dataset.Store(), start, *quietFlag)
	if !*quietFlag {
		fmt.Printf("Calling Commit(), elapsed time: %.2f secs\n", SecsSince(start))
	}
	_, ok = dataset.Commit(types.NewRef(nomsQtRoot.Ref()))
	d.Chk.True(ok, "Could not commit due to conflicting edit")
	if !*quietFlag {
		fmt.Printf("Commit completed, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
	}
	fmt.Println("QuadTree ref:", nomsQtRoot.Ref())
}

func SecsSince(start time.Time) float64 {
	return time.Now().Sub(start).Seconds()
}
