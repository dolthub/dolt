package main

import (
	geo "github.com/attic-labs/noms/clients/gen/sha1_3bfd4da1c27a6472279b96d731b47e58e8832dee"
	"github.com/attic-labs/noms/clients/util"
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

func (qt *SQuadTree) Query(p geo.GeopositionDef, kilometers float64) (geo.GeorectangleDef, []Incident) {
	r := util.BoundingRectangle(p, kilometers)
	nodes := qt.Search(r, p, float32(kilometers))
	return r, nodes
}

func (qt *SQuadTree) Search(r geo.GeorectangleDef, p geo.GeopositionDef, kilometers float32) []Incident {
	nodes := []Incident{}
	if qt.Tiles().Len() > 0 {
		for _, q := range quadrants {
			tile := qt.Tiles().Get(q)
			if util.IntersectsRect(tile.Georectangle().Def(), r) {
				tnodes := tile.Search(r, p, kilometers)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if qt.Nodes().Len() > 0 {
		qt.Nodes().Iter(func(n Incident, i uint64) bool {
			if util.DistanceTo(p, n.Geoposition().Def()) < kilometers {
				nodes = append(nodes, n)
			}
			return false
		})
	}

	return nodes
}
