package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/attic-labs/noms/clients/common"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/noms/walk"
)

var (
	datasFlags  = datas.NewFlags()
	inputRefStr = flag.String("input-ref", "", "ref to list containing nodes")
	outputDs    = flag.String("output-ds", "", "dataset to store data in.")
	quietFlag   = flag.Bool("quiet", false, "suppress printing of progress statements")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Usage = func() {
		fmt.Printf("Usage: %s -ldb=/path/to/db -input-ref=sha1-xyz -output-ds=quadtree\n\n", os.Args[0])
		flag.PrintDefaults()
	}

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

	nChan := make(chan *common.NodeDef, 1024)
	nodesConverted := uint32(0)

	type hasGeoposition interface {
		Geoposition() common.Geoposition
	}

	go func() {
		cs := dataset.Store()
		walk.SomeP(types.ReadValue(inputRef, cs), cs, func(v types.Value) (stop bool) {
			var g common.Geoposition

			switch v := v.(type) {
			case hasGeoposition:
				g = v.Geoposition()
			case types.Struct:
				if mg, ok := v.MaybeGet("geo"); ok {
					if mg, ok := mg.(common.Geoposition); ok {
						g = mg
						break
					}
				}
			default:
				return
			}

			// TODO: This check is mega bummer. We really only want to consider RefOfStruct, but it's complicated to filter the case of an inline struct out.
			if !cs.Has(v.Ref()) {
				return
			}

			stop = true

			nodeDef := &common.NodeDef{Geoposition: g.Def(), Reference: v.Ref()}
			nChan <- nodeDef
			nConverted := atomic.AddUint32(&nodesConverted, 1)
			if !*quietFlag && nConverted%1e5 == 0 {
				fmt.Printf("Nodes Converted: %d, elapsed time: %.2f secs\n", nodesConverted, SecsSince(start))
			}
			return
		}, 64)
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
