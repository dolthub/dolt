package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	datasFlags   = datas.NewFlags()
	inputRefStr  = flag.String("input-ref", "", "ref to find photos from within input chunkstore")
	outputDs     = flag.String("output-ds", "", "dataset to store data in.")
	limitFlag    = flag.Int("limit", math.MaxInt32, "limit size of quadtree")
	quietFlag    = flag.Bool("quiet", false, "suppress printing of progress statements")
	nodetypeFlag = flag.String("nodetype", "", "type of object to insert into quadtree")
	commit       = flag.Bool("commit", true, "commit the quadtree to nomsdb")
)

func main() {
	flag.Parse()
	start := time.Now()

	datastore, ok := datasFlags.CreateDataStore()
	if !ok || *inputRefStr == "" || *outputDs == "" || *nodetypeFlag == "" {
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
	fmt.Printf("quadTreeRoot: %+v\n", qtRoot.Georectangle)

	if util.MaybeStartCPUProfile() {
		defer util.StopCPUProfile()
	}

	val := types.ReadValue(inputRef, dataset.Store())
	list := ListOfNodeFromVal(val)
	if !*quietFlag {
		fmt.Printf("Reading from nodeList: %d items, elapsed time: %.2f secs\n", list.Len(), secsSince(start))
	}

	nodesAppended := 0
	list.Iter(func(n Node) bool {
		nodeDef := NodeDef{Geoposition: n.Geoposition().Def(), Reference: n.Ref()}
		qtRoot.Append(&nodeDef)
		nodesAppended++
		if !*quietFlag && nodesAppended%1e5 == 0 {
			fmt.Printf("Nodes Appended: %d, elapsed time: %.2f secs\n", nodesAppended, secsSince(start))
			qtRoot.Analyze()
		}
		return nodesAppended >= *limitFlag
	})

	if !*quietFlag {
		fmt.Printf("Nodes Appended: %d, elapsed time: %.2f secs\n", nodesAppended, secsSince(start))
		qtRoot.Analyze()
	}

	if *commit {
		_, ok = dataset.Commit(qtRoot.New().NomsValue())
		d.Chk.True(ok, "Could not commit due to conflicting edit")
		fmt.Printf("Commit completed, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
	}

	fmt.Println(dataset.Store().Root().String())
}
