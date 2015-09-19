package main

import (
	"github.com/attic-labs/noms/types"
)

const (
	maxNodes = 16
	maxDepth = 10
	tl       = "topleft"
	bl       = "bottomLeft"
	tr       = "topRight"
	br       = "bottomRight"
)

var (
	tlnv              = types.NewString(tl)
	blnv              = types.NewString(bl)
	trnv              = types.NewString(tr)
	brnv              = types.NewString(br)
	nomsTilePositions = []types.String{tlnv, blnv, trnv, brnv}
)

type TraverseCb func(qt QuadTree) (stop bool)

func (qt QuadTree) Traverse(cb TraverseCb) {
	if !cb(qt) && qt.Children().Len() > 0 {
		for _, k := range nomsTilePositions {
			child := qt.Children().Get(k)
			child.Traverse(cb)
		}
	}
}

// Query is an example for searching for nodes by position
func (qt QuadTree) Query(r Georectangle) []Geonode {
	nodes := []Geonode{}
	if qt.Children().Len() > 0 {
		children := qt.Children()
		for _, k := range nomsTilePositions {
			child := children.Get(k)
			if r.IntersectsRect(child.Georectangle()) {
				cnodes := child.Query(r)
				nodes = append(nodes, cnodes...)
			}
		}
	} else if qt.Nodes().Len() > 0 {
		geoNodes := qt.Nodes()
		for i := uint64(0); i < geoNodes.Len(); i++ {
			geoNode := geoNodes.Get(i)
			if r.ContainsPoint(geoNode.Geoposition()) {
				nodes = append(nodes, geoNode)
			}
		}
	}

	return nodes
}

func (qt QuadTree) hasChildren() bool {
	children := qt.Children()
	return children.Len() > 0
}
