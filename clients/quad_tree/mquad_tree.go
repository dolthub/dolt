package main

import (
	"fmt"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
	"github.com/attic-labs/noms/types"
)

var (
    emptyNomsList     = types.NewList()
    emptyNomsMap      = types.NewMap()
)

type ValueNode struct {
	Geopos    Geoposition
	Reference types.Ref
}

type QtTiles struct {
	tl, bl, tr, br *MQuadTree
}

func (tiles *QtTiles) childContaining(p Geoposition) *MQuadTree {
	if tiles.tl.Rect.ContainsPoint(p) {
		return tiles.tl
	} else if tiles.bl.Rect.ContainsPoint(p) {
		return tiles.bl
	} else if tiles.tr.Rect.ContainsPoint(p) {
		return tiles.tr
	} else if tiles.br.Rect.ContainsPoint(p) {
		return tiles.br
	}
	return nil
}

type MQuadTree struct {
	Path           string
	Depth          uint8
	NumDescendents uint64
	Nodes          []*ValueNode
	Tiles       *QtTiles
	Rect           Georectangle
}

type MTraverseCb func(qt *MQuadTree) (stop bool)

func (qt *MQuadTree) Traverse(cb MTraverseCb) {
	if !cb(qt) && qt.Tiles != nil {
		qt.Tiles.tl.Traverse(cb)
		qt.Tiles.bl.Traverse(cb)
		qt.Tiles.tr.Traverse(cb)
		qt.Tiles.br.Traverse(cb)
	}
}

// Append adds g to MQuadTree
func (qt *MQuadTree) Append(vn *ValueNode) bool {
	var appended bool

	if qt.Tiles != nil {
		child := qt.Tiles.childContaining(vn.Geopos)
		if child != nil {
			if child.Append(vn) {
				qt.NumDescendents++
				appended = true
			}
		} else {
			if *verboseFlag {
				fmt.Println("Failed to find suitable child in path:", qt.Path, "for node:", vn.Geopos)
			}
		}
	} else if len(qt.Nodes) < maxNodes || qt.Depth == maxDepth {
        qt.Nodes = append(qt.Nodes, vn)
        appended = true
	} else {
		qt.split()
        qt.NumDescendents = maxNodes
        appended = qt.Append(vn)
	}

	return appended
}

// split() creates a child quadTree for each quadrant in qt and re-distributes qt's nodes
// among the four children
func (qt *MQuadTree) split() {
	d.Exp.False(qt.Tiles != nil, "attempt to Split QuadTree that already has children")

	qt.Tiles = qt.makeChildren()
	for _, vn := range qt.Nodes {
		child := qt.Tiles.childContaining(vn.Geopos)
		if child != nil {
			child.Append(vn)
		}
	}

	qt.NumDescendents = uint64(len(qt.Nodes))
	qt.Nodes = []*ValueNode{}
}

func CreateNewMQuadTree(depth uint8, path string, rect Georectangle) *MQuadTree {
    nodes := make([]*ValueNode, 0, maxNodes)
	qt := MQuadTree{
		Nodes:          nodes,
        Tiles:       nil,
		Depth:          depth,
		NumDescendents: 0,
		Path:           path,
		Rect:           rect,
	}
	return &qt
}

// makeChildren() handles the dirty work of splitting up the Georectangle in qt
// into 4 quadrants
func (qt *MQuadTree) makeChildren() *QtTiles {
	if *verboseFlag {
		//         fmt.Println("makeChildren: entered")
	}
	children := new(QtTiles)
	tlRect, blRect, trRect, brRect := qt.Rect.Split()
	children.tl = CreateNewMQuadTree(qt.Depth+1, qt.Path+"a", tlRect)
	children.bl = CreateNewMQuadTree(qt.Depth+1, qt.Path+"b", blRect)
	children.tr = CreateNewMQuadTree(qt.Depth+1, qt.Path+"c", trRect)
	children.br = CreateNewMQuadTree(qt.Depth+1, qt.Path+"d", brRect)

	return children
}

func (qt *MQuadTree) SaveToNoms(cs chunks.ChunkSink) (*ref.Ref, types.Value) {
    var nomsNodes types.List
    var nomsChildren types.Map
    //    fmt.Printf("HERE depth: %2d, hasChildren: %5t, numNodes: %2d, path: %s\n", qt.Depth, qt.hasChildren(), len(qt.Nodes), qt.Path)

    if qt.Tiles != nil {
        ref, _ := qt.Tiles.tl.SaveToNoms(cs)
        tlRef := types.Ref{R: *ref}
        ref, _ = qt.Tiles.bl.SaveToNoms(cs)
        blRef := types.Ref{R: *ref}
        ref, _ = qt.Tiles.tr.SaveToNoms(cs)
        trRef := types.Ref{R: *ref}
        ref, _ = qt.Tiles.br.SaveToNoms(cs)
        brRef := types.Ref{R: *ref}
        nomsChildren = types.NewMap(tlnv, tlRef, blnv, blRef, trnv, trRef, brnv, brRef)
        nomsNodes = emptyNomsList
        // cut the children loose so they can be garbage collected
        qt.Tiles = nil
    } else {
        //        vRefs := make([]*types.Ref, 0, len(qt.Nodes))
        nomsNodes = types.NewList()
        for _, n := range qt.Nodes {
            //            vRefs = append(vRefs, &n.Reference)
            nomsNodes = nomsNodes.Append(n.Reference)
        }
        //        l = types.NewList(vRefs...)
        nomsChildren = emptyNomsMap
    }
    nqt := NewQuadTree().
    SetGeorectangle(qt.Rect).
    SetDepth(types.UInt8(qt.Depth)).
    SetNumDescendents(types.UInt64(qt.NumDescendents)).
    SetPath(types.NewString(qt.Path))
    nqt = QuadTreeFromVal(setValueInNomsMap(nqt.NomsValue(), "Nodes", nomsNodes))
    nqt = QuadTreeFromVal(setValueInNomsMap(nqt.NomsValue(), "Children", nomsChildren))
    //    fmt.Printf("Writing %+v\n", nqt)
    nv := nqt.NomsValue()
    r := types.WriteValue(nv, cs)
    //    fmt.Printf("Wrote %+v\n\n", r)
    return &r, nv
}

func setValueInNomsMap(m types.Value, field string, value types.Value) types.Value {
	return m.(types.Map).Set(types.NewString(field), value)
}
