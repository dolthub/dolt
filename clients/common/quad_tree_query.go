package common

import "github.com/attic-labs/noms/types"

func (qt *SQuadTree) Query(p GeopositionDef, kilometers float64, vr types.ValueReader) (GeorectangleDef, []Incident) {
	r := BoundingRectangle(p, kilometers)
	nodes := qt.Search(r, p, float32(kilometers), vr)
	return r, nodes
}

func (qt *SQuadTree) Search(r GeorectangleDef, p GeopositionDef, kilometers float32, vr types.ValueReader) []Incident {
	nodes := []Incident{}
	if qt.Tiles().Len() > 0 {
		for _, q := range quadrants {
			tile := qt.Tiles().Get(q).TargetValue(vr)
			if IntersectsRect(tile.Georectangle().Def(), r) {
				tnodes := tile.Search(r, p, kilometers, vr)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if qt.Nodes().Len() > 0 {
		qt.Nodes().Iter(func(r RefOfValue, i uint64) bool {
			incident := r.TargetValue(vr).(Incident)
			if DistanceTo(p, incident.Geoposition().Def()) < kilometers {
				nodes = append(nodes, incident)
			}
			return false
		})
	}

	return nodes
}
