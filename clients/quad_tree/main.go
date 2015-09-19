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
	datasFlags      = datas.NewFlags()
	inputRefStr     = flag.String("input-ref", "", "ref to find photos from within input chunkstore")
	outputDs        = flag.String("output-ds", "", "dataset to store data in.")
	limitFlag       = flag.Int("limit", math.MaxInt32, "limit size of quadtree")
	verboseFlag     = flag.Bool("verbose", true, "print progress statements")
	geonodetypeFlag = flag.String("geonodetype", "", "type of object to insert into quadtree")
	commit          = flag.Bool("commit", true, "commit the quadtree to nomsdb")
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

	gr := CreateNewGeorectangle(37.83, -122.52, 37.70, -122.36)
	quadTreeRoot := CreateNewMQuadTree(0, "", gr)
	fmt.Println("quadTreeRoot:", quadTreeRoot.Rect)

	dataset := dataset.NewDataset(datastore, *outputDs)

	nodesAdded := 0

	util.MaybeStartCPUProfile()
	util.MaybeWriteMemProfile()

	if *verboseFlag {
        fmt.Printf("Starting read, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
    }

	timesCalled := 0
	types.Some(inputRef, datastore, func(f types.Future) (skip bool) {
		limitReached := nodesAdded >= *limitFlag
		timesCalled++
		if !limitReached {
			v := f.Deref(datastore)
			if v, ok := v.(types.Map); ok && getStringValueInNomsMap(v, "$name") == *geonodetypeFlag {
				skip = true
				vn := &ValueNode{
					Geopos:    GeonodeFromVal(v).Geoposition(),
					Reference: types.Ref{R: v.Ref()},
				}
				quadTreeRoot.Append(vn)
				nodesAdded++
				if *verboseFlag && nodesAdded > 0 && nodesAdded%1e4 == 0 {
					fmt.Printf("Added node %d, tree contains %d nodes, elapsed time: %.2f secs\n", nodesAdded, quadTreeRoot.NumDescendents, time.Now().Sub(start).Seconds())
					if nodesAdded%1e4 == 0 {
						quadTreeRoot.analyze()
					}
				}
			}
		} else {
			skip = true
		}
		return
	})

	util.StopCPUProfile()

	if *verboseFlag {
        fmt.Printf("Tree construction completed, callbacks: %d, elapsed time: %.2f secs\n", timesCalled, time.Now().Sub(start).Seconds())
    }
	quadTreeRoot.analyze()

	if *commit {
		cs := dataset.Store().ChunkStore
		_, nomsQT := quadTreeRoot.SaveToNoms(cs)
		_, ok = dataset.Commit(nomsQT)
		d.Exp.True(ok, "Could not commit due to conflicting edit")
		fmt.Printf("Commit completed, elapsed time: %.2f secs\n", time.Now().Sub(start).Seconds())
	}

	fmt.Println(dataset.Store().Root().String())
}

func (qt MQuadTree) analyze() {
	qtCount := 0
	qtEmpty := 0
	qtCountsByLevel := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	nodeCountsByLevel := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	qtEmptyByLevel := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	mDepth := uint8(0)
	qt.Traverse(func(qt *MQuadTree) (stop bool) {
		qtCount++
        nodeCount := len(qt.Nodes)
		depth := uint8(qt.Depth)
		mDepth = max(mDepth, depth)
		qtCountsByLevel[depth]++
		nodeCountsByLevel[depth] += uint64(nodeCount)
		if qt.Tiles == nil && nodeCount == 0 {
			qtEmpty++
			qtEmptyByLevel[depth]++
		}
		return false
	})
	fmt.Printf("qtCount: %d, emptyQtCount: %d, qtCountByLevel: %d, nodeCountsByLevel: %v, emptyQtCountByLevel: %v, maxDepth: %d\n",
		qtCount, qtEmpty, qtCountsByLevel, nodeCountsByLevel, qtEmptyByLevel, mDepth)
}

func getStringValueInNomsMap(m types.Value, field string) string {
	return m.(types.Map).Get(types.NewString(field)).(types.String).String()
}

func max(x, y uint8) uint8 {
	if x >= y {
		return x
	} else {
		return y
	}
}
