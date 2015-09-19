package main

import (
	"fmt"
	"github.com/attic-labs/noms/types"
)

// LessThan returns true if it is above and to the left of "o"
func (p Geoposition) LessThan(o Geoposition) bool {
	return p.Latitude() > o.Latitude() && p.Longitude() < o.Longitude()
}

// GreaterThan returns true if it's below and to the right of "o"
func (p Geoposition) GreaterThan(o Geoposition) bool {
	return p.Latitude() < o.Latitude() && p.Longitude() > o.Longitude()
}

// LessThanOrEqual returns true if it is above and to the left of "o"
func (p Geoposition) LessThanOrEqual(o Geoposition) bool {
	return p.Latitude() >= o.Latitude() && p.Longitude() <= o.Longitude()
}

// GreaterThanOrEqual returns true if it's below and to the right of "o"
func (p Geoposition) GreaterThanOrEqual(o Geoposition) bool {
	return p.Latitude() <= o.Latitude() && p.Longitude() >= o.Longitude()
}

func (p Geoposition) String() string {
	return fmt.Sprintf("Geoposition(lat: %0.3f, lon: %0.3f)", p.Latitude(), p.Longitude())
}

// CreateNewGeorectangle is a convenient way to call NewGeorectangle using the topleft and bottom right coordinates.
func CreateNewGeorectangle(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon float32) Georectangle {
	tl := NewGeoposition().SetLatitude(types.Float32(topLeftLat)).SetLongitude(types.Float32(topLeftLon))
	br := NewGeoposition().SetLatitude(types.Float32(bottomRightLat)).SetLongitude(types.Float32(bottomRightLon))
	return NewGeorectangle().SetTopLeft(tl).SetBottomRight(br)
}

// IntersectsRect returns true if "o" intersects r
func (r Georectangle) IntersectsRect(o Georectangle) bool {
	return r.TopLeft().LessThan(o.BottomRight()) && r.BottomRight().GreaterThan(o.TopLeft())
}

// ContainsRect returns true if "o" is within r
func (r Georectangle) ContainsRect(o Georectangle) bool {
	return r.TopLeft().LessThanOrEqual(o.TopLeft()) && r.BottomRight().GreaterThanOrEqual(o.BottomRight())
}

// ContainsPoint returns true if p is contained in r
func (r Georectangle) ContainsPoint(p Geoposition) bool {
	return r.TopLeft().LessThanOrEqual(p) && r.BottomRight().GreaterThan(p)
}

// Split creates one new rectangle for each quadrant in "r"
func (r Georectangle) Split() (tlRect, blRect, trRect, brRect Georectangle) {
	//    fmt.Println("Splitting:", r)
	maxLat := float32(r.TopLeft().Latitude())
	minLon := float32(r.TopLeft().Longitude())
	minLat := float32(r.BottomRight().Latitude())
	maxLon := float32(r.BottomRight().Longitude())
	midLat := ((maxLat - minLat) / 2) + minLat
	midLon := ((maxLon - minLon) / 2) + minLon

	tlRect = CreateNewGeorectangle(maxLat, minLon, midLat, midLon)
	blRect = CreateNewGeorectangle(midLat, minLon, minLat, midLon)
	trRect = CreateNewGeorectangle(maxLat, midLon, midLat, maxLon)
	brRect = CreateNewGeorectangle(midLat, midLon, minLat, maxLon)

	return
}

func (r Georectangle) String() string {
	return fmt.Sprintf("Georectangle(topLeft(%s), bottomRight(%s))", r.TopLeft(), r.BottomRight())
}
