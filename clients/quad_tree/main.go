package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
	datasFlags      = datas.NewFlags()
	inputRefStr     = flag.String("input-ref", "", "ref to find photos from within input chunkstore")
	outputDs        = flag.String("output-ds", "", "dataset to store data in.")
	limitFlag       = flag.Int("limit", math.MaxInt32, "limit size of quadtree")
	verboseFlag     = flag.Bool("verbose", true, "print progress statements")
	geonodetypeFlag = flag.String("geonodetype", "", "type of object to insert into quadtree")
)

func main() {
	flag.Parse()
	start := time.Now()

	datastore, ok := datasFlags.CreateDataStore()
	if !ok || *inputRefStr == "" || *outputDs == "" || *geonodetypeFlag == "" {
		flag.Usage()
		return
	}
	defer datastore.Close()

	var inputRef ref.Ref
	err := d.Try(func() {
		inputRef = ref.Parse(*inputRefStr)
	})
	if err != nil {
		log.Fatalf("Invalid ref: %v", *inputRefStr)
	}

	gr := CreateNewGeorectangle(37.82, -122.52, 37.70, -122.36)
	quadTreeRoot := CreateNewQuadTree(gr, 0, "")
	fmt.Println("quadTreeRoot:", quadTreeRoot.Georectangle())

	dataset := dataset.NewDataset(datastore, *outputDs)

	nodesAdded := 0

	types.Some(inputRef, datastore, func(f types.Future) (skip bool) {
		skip = nodesAdded >= *limitFlag
		if !skip {
			v := f.Deref(datastore)
			if v, ok := v.(types.Map); ok && getStringValueInNomsMap(v, "$name") == *geonodetypeFlag {
				gn := GeonodeFromVal(v)
				nodesAdded++
				quadTreeRoot = quadTreeRoot.Append(gn)
				if *verboseFlag && nodesAdded > 0 && nodesAdded%1000 == 0 {
					fmt.Printf("Added node %d, tree contains %d nodes, elapsed time: %.2f secs\n", nodesAdded, quadTreeRoot.NumDescendents(), time.Now().Sub(start).Seconds())
				}
			}
		}
		return
	})

	fmt.Printf("Tree completed, starting commit, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
	_, ok = dataset.Commit(quadTreeRoot.NomsValue())
	d.Exp.True(ok, "Could not commit due to conflicting edit")

	fmt.Printf("Commit completed, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
	//    result := quadTreeRoot.Query(gr)
	//    fmt.Println("found nodes, count:", len(result))

	fmt.Println(dataset.Store().Root().String())
}

func getStringValueInNomsMap(m types.Value, field string) string {
	return m.(types.Map).Get(types.NewString(field)).(types.String).String()
}
