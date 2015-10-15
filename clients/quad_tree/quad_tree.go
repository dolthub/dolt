package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/attic-labs/noms/chunks"
	geo "github.com/attic-labs/noms/clients/gen/sha1_3bfd4da1c27a6472279b96d731b47e58e8832dee"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/types"
)

const (
	maxNodes = 16
	maxDepth = 10
	tl       = "TopLeft"
	bl       = "BottomLeft"
	tr       = "TopRight"
	br       = "BottomRight"
)

var (
	quadrants = []string{tl, bl, tr, br}
	saveCount = 0
)

// Query returns a slice of Nodes that are a maximum of "kilometers" away from "p"
func (qt *QuadTree) Query(p geo.GeopositionDef, kilometers float64) (geo.GeorectangleDef, []Node) {
	r := util.BoundingRectangle(p, kilometers)
	nodes := qt.Search(r, p, float32(kilometers))
	return r, nodes
}

// Search returns a slice of Nodes that are within "r" and a maximum of "kilometers" away from "p"
func (qt *QuadTree) Search(r geo.GeorectangleDef, p geo.GeopositionDef, kilometers float32) []Node {
	nodes := []Node{}
	if qt.Tiles().Len() > 0 {
		for _, q := range quadrants {
			tile := qt.Tiles().Get(q)
			if util.IntersectsRect(tile.Georectangle().Def(), r) {
				tnodes := tile.Search(r, p, kilometers)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if qt.Nodes().Len() > 0 {
		qt.Nodes().Iter(func(n Node, i uint64) bool {
			if util.DistanceTo(p, n.Geoposition().Def()) < kilometers {
				nodes = append(nodes, n)
			}
			return false
		})
	}

	return nodes
}

// Query returns a slice of NodeDefs that are a maximum of "kilometers" away from "p"
func (qt *QuadTreeDef) Query(p geo.GeopositionDef, kilometers float64) ListOfNodeDef {
	r := util.BoundingRectangle(p, kilometers)
	return qt.Search(r, p, float32(kilometers))
}

// Search returns a slice of NodeDefs that are within "r" and a maximum of "kilometers" away from "p"
func (qt QuadTreeDef) Search(r geo.GeorectangleDef, p geo.GeopositionDef, kilometers float32) ListOfNodeDef {
	nodes := ListOfNodeDef{}
	if qt.hasTiles() {
		for _, q := range quadrants {
			tile := qt.Tiles[q]
			if util.IntersectsRect(tile.Georectangle, r) {
				tnodes := tile.Search(r, p, kilometers)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if len(qt.Nodes) > 0 {
		for _, n := range qt.Nodes {
			if util.DistanceTo(p, n.Geoposition) < kilometers {
				nodes = append(nodes, n)
			}
		}
	}

	return nodes
}

func (qt *QuadTreeDef) hasTiles() bool {
	return len(qt.Tiles) > 0
}

// MTraverseCb declares type of function used as callback in Traverse method.
type MTraverseCb func(qt *QuadTreeDef) (stop bool)

// Traverse calls the func "cb" for each tile in the QuadTreeDef in "prefix" order.
func (qt *QuadTreeDef) Traverse(cb MTraverseCb) {
	if !cb(qt) && len(qt.Tiles) > 0 {
		for _, q := range quadrants {
			tile := qt.Tiles[q]
			tile.Traverse(cb)
		}
	}
}

// Analyze prints some useful stats for debugging QuadTreeDef trees.
func (qt *QuadTreeDef) Analyze() {
	qtCount := 0
	qtEmpty := 0
	qtCountsByLevel := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	nodeCountsByLevel := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	qtEmptyByLevel := []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	mDepth := uint8(0)
	qt.Traverse(func(qt *QuadTreeDef) (stop bool) {
		qtCount++
		nodeCount := len(qt.Nodes)
		depth := uint8(qt.Depth)
		mDepth = max(mDepth, depth)
		qtCountsByLevel[depth]++
		nodeCountsByLevel[depth] += uint64(nodeCount)
		if !qt.hasTiles() && nodeCount == 0 {
			qtEmpty++
			qtEmptyByLevel[depth]++
		}
		return false
	})
	fmt.Printf("qtCount: %d, emptyQtCount: %d, qtCountByLevel: %d, nodeCountsByLevel: %v, emptyQtCountByLevel: %v, maxDepth: %d\n",
		qtCount, qtEmpty, qtCountsByLevel, nodeCountsByLevel, qtEmptyByLevel, mDepth)
}

// Append appends a NodeDef to MQuadTree
func (qt *QuadTreeDef) Append(n *NodeDef) {
	if qt.hasTiles() {
		quadrant, tile := qt.tileContaining(n.Geoposition)
		tile.Append(n)
		qt.Tiles[quadrant] = tile
		qt.NumDescendents++
	} else if len(qt.Nodes) < maxNodes || qt.Depth == maxDepth {
		qt.Nodes = append(qt.Nodes, *n)
	} else {
		qt.split()
		qt.NumDescendents = maxNodes
		qt.Append(n)
	}
}

// split() creates a child quadTree for each quadrant in qt and re-distributes qt's nodes
// among the four children
func (qt *QuadTreeDef) split() {
	d.Exp.False(qt.hasTiles(), "attempt to Split QuadTree that already has tiles")

	qt.makeChildren()
	for _, n := range qt.Nodes {
		quadrant, tile := qt.tileContaining(n.Geoposition)
		tile.Append(&n)
		qt.Tiles[quadrant] = tile
	}

	qt.NumDescendents = uint32(len(qt.Nodes))
	qt.Nodes = ListOfNodeDef{}
}

func (qt *QuadTreeDef) tileContaining(p geo.GeopositionDef) (quadrant string, tile QuadTreeDef) {
	d.Chk.True(qt.hasTiles(), "tileContaining method called on QuadTree node with no tiles")

	if util.ContainsPoint(qt.Tiles[tl].Georectangle, p) {
		quadrant, tile = tl, qt.Tiles[tl]
	} else if util.ContainsPoint(qt.Tiles[bl].Georectangle, p) {
		quadrant, tile = bl, qt.Tiles[bl]
	} else if util.ContainsPoint(qt.Tiles[tr].Georectangle, p) {
		quadrant, tile = tr, qt.Tiles[tr]
	} else if util.ContainsPoint(qt.Tiles[br].Georectangle, p) {
		quadrant, tile = br, qt.Tiles[br]
	}

	return quadrant, tile
}

// CreateNewQuadTreeDef is a convenience method for creating a new QuadTreeDef.
func CreateNewQuadTreeDef(depth uint8, path string, rect geo.GeorectangleDef) QuadTreeDef {
	nodes := make(ListOfNodeDef, 0, maxNodes)
	qt := QuadTreeDef{
		Nodes:          nodes,
		Tiles:          MapOfStringToQuadTreeDef{},
		Depth:          depth,
		NumDescendents: 0,
		Path:           path,
		Georectangle:   rect,
	}
	return qt
}

// makeChildren() handles the dirty work of splitting up the Georectangle in qt
// into 4 quadrants
func (qt *QuadTreeDef) makeChildren() {
	tlRect, blRect, trRect, brRect := util.Split(qt.Georectangle)
	qt.Tiles[tl] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"a", tlRect)
	qt.Tiles[bl] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"b", blRect)
	qt.Tiles[tr] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"c", trRect)
	qt.Tiles[br] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"d", brRect)
}

func (qt *QuadTreeDef) SaveToNoms(cs chunks.ChunkSink, start time.Time) *SQuadTree {
	wChan := make(chan *SQuadTree, 1024)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			for sqt := range wChan {
				types.WriteValue(sqt.NomsValue(), cs)
			}
			wg.Done()
		}()
	}

	if util.MaybeStartCPUProfile() {
		defer util.StopCPUProfile()
	}

	sqt := qt.saveNodeToNoms(wChan, cs, start)
	close(wChan)
	wg.Wait()
	return sqt
}

func (qt *QuadTreeDef) saveNodeToNoms(wChan chan *SQuadTree, cs chunks.ChunkSink, start time.Time) *SQuadTree {
	tileRefs := MapOfStringToRefOfSQuadTreeDef{}
	nrefs := make(ListOfRefOfValueDef, 0, len(qt.Nodes))
	if qt.hasTiles() {
		for q, tile := range qt.Tiles {
			child := tile.saveNodeToNoms(wChan, cs, start)
			ref := child.NomsValue().Ref()
			tileRefs[q] = ref
		}
	} else if len(qt.Nodes) > 0 {
		for _, n := range qt.Nodes {
			nrefs = append(nrefs, n.Reference)
		}
	}
	sqt := SQuadTreeDef{
		Nodes:          nrefs,
		Tiles:          tileRefs,
		Depth:          qt.Depth,
		NumDescendents: qt.NumDescendents,
		Path:           qt.Path,
		Georectangle:   qt.Georectangle,
	}.New()
	sqtp := &sqt

	wChan <- sqtp
	saveCount++
	if saveCount%1e4 == 0 && !*quietFlag {
		fmt.Printf("Nodes Saved: %d, elapsed time: %.2f secs\n", saveCount, secsSince(start))
	}
	return sqtp
}

func max(x, y uint8) uint8 {
	if x >= y {
		return x
	}
	return y
}

func secsSince(start time.Time) float64 {
	return time.Now().Sub(start).Seconds()
}
