package main

import (
	//    "fmt"
	"fmt"
	"github.com/attic-labs/noms/d"
	"time"
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
)

// Query returns a slice of Nodes that are a maximum of "kilometers" away from "p"
func (qt *QuadTree) Query(p GeopositionDef, kilometers float64) (GeorectangleDef, []Node) {
	r := p.BoundingRectangle(kilometers)
	fmt.Printf("Query, p: %v, klms: %f, boundingRect: %v", p, kilometers, r)
	nodes := qt.Search(r, p, float32(kilometers))
	return r, nodes
}

// Search returns a slice of Nodes that are within "r" and a maximum of "kilometers" away from "p"
func (qt *QuadTree) Search(r GeorectangleDef, p GeopositionDef, kilometers float32) []Node {
	fmt.Printf("Search, qt: %p, path %s, depth: %d\n", qt, qt.Path(), qt.Depth())
	nodes := []Node{}
	if qt.Tiles().Len() > 0 {
		for _, q := range quadrants {
			tile := qt.Tiles().Get(q)
			fmt.Printf("Search: testing quadrant: %11s, depth: %d, path: %s\n", q, tile.Depth(), tile.Path())
			if tile.Georectangle().Def().IntersectsRect(r) {
				tnodes := tile.Search(r, p, kilometers)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if qt.Nodes().Len() > 0 {
		qt.Nodes().Iter(func(n Node) bool {
			if p.DistanceTo(n.Geoposition().Def()) < kilometers {
				nodes = append(nodes, n)
			}
			return false
		})
	}

	return nodes
}

// Query returns a slice of NodeDefs that are a maximum of "kilometers" away from "p"
func (qt *QuadTreeDef) Query(p GeopositionDef, kilometers float64) ListOfNodeDef {
	r := p.BoundingRectangle(kilometers)
	return qt.Search(r, p, float32(kilometers))
}

// Search returns a slice of NodeDefs that are within "r" and a maximum of "kilometers" away from "p"
func (qt QuadTreeDef) Search(r GeorectangleDef, p GeopositionDef, kilometers float32) ListOfNodeDef {
	nodes := ListOfNodeDef{}
	if qt.hasTiles() {
		for _, q := range quadrants {
			tile := qt.Tiles[q]
			if tile.Georectangle.IntersectsRect(r) {
				tnodes := tile.Search(r, p, kilometers)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if len(qt.Nodes) > 0 {
		for _, n := range qt.Nodes {
			if p.DistanceTo(n.Geoposition) < kilometers {
				nodes = append(nodes, n)
			}
		}
	}

	return nodes
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

func (qt *QuadTreeDef) tileContaining(p GeopositionDef) (quadrant string, tile QuadTreeDef) {
	d.Chk.True(qt.hasTiles(), "tileContaining method called on QuadTree node with no tiles")

	if qt.Tiles[tl].Georectangle.ContainsPoint(p) {
		quadrant, tile = tl, qt.Tiles[tl]
	} else if qt.Tiles[bl].Georectangle.ContainsPoint(p) {
		quadrant, tile = bl, qt.Tiles[bl]
	} else if qt.Tiles[tr].Georectangle.ContainsPoint(p) {
		quadrant, tile = tr, qt.Tiles[tr]
	} else if qt.Tiles[br].Georectangle.ContainsPoint(p) {
		quadrant, tile = br, qt.Tiles[br]
	}

	return quadrant, tile
}

// CreateNewQuadTreeDef is a convenience method for creating a new QuadTreeDef.
func CreateNewQuadTreeDef(depth uint8, path string, rect GeorectangleDef) QuadTreeDef {
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
	tlRect, blRect, trRect, brRect := qt.Georectangle.Split()
	qt.Tiles[tl] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"a", tlRect)
	qt.Tiles[bl] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"b", blRect)
	qt.Tiles[tr] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"c", trRect)
	qt.Tiles[br] = CreateNewQuadTreeDef(qt.Depth+1, qt.Path+"d", brRect)
}

func (qt *QuadTreeDef) hasTiles() bool {
	return len(qt.Tiles) > 0
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
