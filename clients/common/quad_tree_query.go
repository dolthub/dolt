package common

import "github.com/attic-labs/noms/datas"

func (qt *SQuadTree) Query(p GeopositionDef, kilometers float64, ds datas.DataStore) (GeorectangleDef, []Incident) {
	r := BoundingRectangle(p, kilometers)
	nodes := qt.Search(r, p, float32(kilometers), ds)
	return r, nodes
}

func (qt *SQuadTree) Search(r GeorectangleDef, p GeopositionDef, kilometers float32, ds datas.DataStore) []Incident {
	nodes := []Incident{}
	if qt.Tiles().Len() > 0 {
		for _, q := range quadrants {
			tile := qt.Tiles().Get(q).TargetValue(ds)
			if IntersectsRect(tile.Georectangle().Def(), r) {
				tnodes := tile.Search(r, p, kilometers, ds)
				nodes = append(nodes, tnodes...)
			}
		}
	} else if qt.Nodes().Len() > 0 {
		qt.Nodes().Iter(func(r RefOfValue, i uint64) bool {
			incident := r.TargetValue(ds).(Incident)
			if DistanceTo(p, incident.Geoposition().Def()) < kilometers {
				nodes = append(nodes, incident)
			}
			return false
		})
	}

	return nodes
}
