package main

import (
	"fmt"
	"github.com/attic-labs/noms/d"
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
	tilePositions     = []string{tl, bl, tr, br}
	nomsTilePositions = []types.String{tlnv, blnv, trnv, brnv}
	positionLetters   = map[string]string{tl: "a", bl: "b", tr: "c", br: "d"}
)

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

// CreateNewQuadTree is a convenience method to wrap NewQuadTree
func CreateNewQuadTree(gr Georectangle, depth uint8, path string) QuadTree {
	qt := NewQuadTree().
		SetGeorectangle(gr).
		SetChildren(NewMapOfStringToQuadTree()).
		SetNodes(NewListOfGeonode()).
		SetDepth(types.UInt8(depth)).
		SetNumDescendents(types.UInt64(0)).
		SetPath(types.NewString(path))
	return qt
}

// Append creates a new immutable quadTree containing that contains g
func (qt QuadTree) Append(g Geonode) (newQt QuadTree) {
	if qt.hasChildren() {
		children := qt.Children()
		foundContainingRect := false
		for _, k := range nomsTilePositions {
			child := children.Get(k)
			if child.Georectangle().ContainsPoint(g.Geoposition()) {
				foundContainingRect = true
				children = children.Set(k, child.Append(g))
				break
			}
		}
		if !foundContainingRect {
			if *verboseFlag {
				fmt.Println("Failed to find suitable child in path:", qt.Path(), "for node:", g.Geoposition())
			}
		}
		numDesc := calcNumDescendents(children)
		newQt = qt.copy(nil, nil, &children, &numDesc)
	} else if qt.Nodes().Len() < maxNodes || qt.Depth() == maxDepth {
		//        fmt.Printf("Adding node to leaf: %s(%d)\n", qt.Path(), qt.Depth())
		geoNodes := qt.Nodes().Append(g)
		newQt = qt.copy(nil, &geoNodes, nil, nil)
	} else {
		newQt = qt.split().Append(g)
	}

	return newQt
}

// split() creates a child quadTree for each quadrant in qt and re-distributes qt's nodes
// among the four children
func (qt QuadTree) split() (nqt QuadTree) {
	d.Exp.False(qt.hasChildren(), "attempt to Split QuadTree that already has children")

	children := qt.makeChildren()
	nodes := qt.Nodes()
	for i := uint64(0); i < nodes.Len(); i++ {
		node := nodes.Get(i)
		for k, child := range children {
			if child.Georectangle().ContainsPoint(node.Geoposition()) {
				children[k] = child.Append(node)
				break
			}
		}
	}

	qtChildren := NewMapOfStringToQuadTree().
		Set(tlnv, children[tl]).
		Set(blnv, children[bl]).
		Set(trnv, children[tr]).
		Set(brnv, children[br])
	geoNodes := NewListOfGeonode()
	numDesc := calcNumDescendents(qtChildren)
	return qt.copy(nil, &geoNodes, &qtChildren, &numDesc)
}

// makeChildren() handles the dirty work of splitting up the Georectangle in qt
// into 4 quadrants
func (qt QuadTree) makeChildren() map[string]QuadTree {
	if *verboseFlag {
		// fmt.Println("making children:")
	}
	children := map[string]QuadTree{}
	depth := uint8(qt.Depth())
	path := qt.Path().String()
	for k, r := range qt.Georectangle().Split() {
		child := CreateNewQuadTree(r, depth+1, path+positionLetters[k])
		children[k] = child
	}

	return children
}

// copy() is a convenient way of creating a copy of a quadTree node with overrides of one or more field values
func (qt QuadTree) copy(rect *Georectangle, nodes *ListOfGeonode, children *MapOfStringToQuadTree, numDesc *uint64) QuadTree {
	var r Georectangle
	if rect == nil {
		r = qt.Georectangle()
	} else {
		r = *rect
	}

	var n ListOfGeonode
	if nodes == nil {
		n = qt.Nodes()
	} else {
		n = *nodes
	}

	var c MapOfStringToQuadTree
	if children == nil {
		c = qt.Children()
	} else {
		c = *children
	}

	var nDesc types.UInt64
	if numDesc == nil {
		nDesc = qt.NumDescendents()
	} else {
		nDesc = types.UInt64(*numDesc)
	}

	return NewQuadTree().SetGeorectangle(r).SetNodes(n).SetChildren(c).SetNumDescendents(nDesc).SetDepth(qt.Depth()).SetPath(qt.Path())
}

func (qt QuadTree) hasChildren() bool {
	children := qt.Children()
	return children.Len() > 0
}

func calcNumDescendents(children MapOfStringToQuadTree) uint64 {
	numDesc := uint64(0)
	for _, tilePos := range nomsTilePositions {
		child := children.Get(tilePos)
		cNumDesc := uint64(child.NumDescendents())
		cNumNodes := child.Nodes().Len()
		numDesc = numDesc + cNumDesc + cNumNodes
	}
	return numDesc
}
