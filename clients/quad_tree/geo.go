package main

import (
	"math"
)

const (
	// EarthRadius the Earth's radius is about 6,371km according to Wikipedia
	EarthRadius = 6371
)

// Latitude64 casts the Latitude value to a float64 for convenience.
func Latitude64(p GeopositionDef) float64 {
	return float64(p.Latitude)
}

// Longitude64 casts the Longitude value to a float64 for convenience.
func Longitude64(p GeopositionDef) float64 {
	return float64(p.Longitude)
}

// TopLeftOf returns true if it is above and to the left of "o"
func TopLeftOf(p GeopositionDef, o GeopositionDef) bool {
	return p.Latitude > o.Latitude && p.Longitude < o.Longitude
}

// BelowRightOf returns true if it's below and to the right of "o"
func BelowRightOf(p GeopositionDef, o GeopositionDef) bool {
	return p.Latitude < o.Latitude && p.Longitude > o.Longitude
}

// TopLeftOrSameOf returns true if it is above and to the left of "o"
func TopLeftOrSameOf(p GeopositionDef, o GeopositionDef) bool {
	return p.Latitude >= o.Latitude && p.Longitude <= o.Longitude
}

// BelowRightOrSameOf returns true if it's below and to the right of "o"
func BelowRightOrSameOf(p GeopositionDef, o GeopositionDef) bool {
	return p.Latitude <= o.Latitude && p.Longitude >= o.Longitude
}

// PointAtDistanceAndBearing returns a Point populated with the lat and lng coordinates
// by transposing the origin point the passed in distance (in kilometers)
// by the passed in compass bearing (in degrees).
// Original Implementation from: http://www.movable-type.co.uk/scripts/latlong.html
func PointAtDistanceAndBearing(p GeopositionDef, dist float64, bearing float64) *GeopositionDef {
	dr := dist / EarthRadius

	bearing = bearing * (math.Pi / 180.0)

	lat1 := Latitude64(p) * (math.Pi / 180.0)
	lng1 := Longitude64(p) * (math.Pi / 180.0)

	lat2Part1 := math.Sin(lat1) * math.Cos(dr)
	lat2Part2 := math.Cos(lat1) * math.Sin(dr) * math.Cos(bearing)

	lat2 := math.Asin(lat2Part1 + lat2Part2)

	lng2Part1 := math.Sin(bearing) * math.Sin(dr) * math.Cos(lat1)
	lng2Part2 := math.Cos(dr) - (math.Sin(lat1) * math.Sin(lat2))

	lng2 := lng1 + math.Atan2(lng2Part1, lng2Part2)
	lng2 = math.Mod((lng2+3*math.Pi), (2*math.Pi)) - math.Pi

	lat2 = lat2 * (180.0 / math.Pi)
	lng2 = lng2 * (180.0 / math.Pi)

	return &GeopositionDef{Latitude: float32(lat2), Longitude: float32(lng2)}
}

// DistanceTo calculates the Haversine distance between two points in kilometers.
// Original Implementation from: http://www.movable-type.co.uk/scripts/latlong.html
func DistanceTo(p GeopositionDef, o GeopositionDef) float32 {
	dLat := (Latitude64(o) - Latitude64(p)) * (math.Pi / 180.0)
	dLon := (Longitude64(o) - Longitude64(p)) * (math.Pi / 180.0)

	lat1 := Latitude64(p) * (math.Pi / 180.0)
	lat2 := Latitude64(o) * (math.Pi / 180.0)

	a1 := math.Sin(dLat/2) * math.Sin(dLat/2)
	a2 := math.Sin(dLon/2) * math.Sin(dLon/2) * math.Cos(lat1) * math.Cos(lat2)

	a := a1 + a2

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return float32(EarthRadius * c)
}

// BoundingRectangle calculates a rectangle whose sides east/west and north/south.
// The center of the rectangle is "p" and each side is "2 * radius" in length.
// A radius value of 1 equals 1 kilometer.
func BoundingRectangle(p GeopositionDef, radius float64) GeorectangleDef {
	northPoint := PointAtDistanceAndBearing(p, radius, 0)
	eastPoint := PointAtDistanceAndBearing(p, radius, 90)
	southPoint := PointAtDistanceAndBearing(p, radius, 180)
	westPoint := PointAtDistanceAndBearing(p, radius, 270)

	return GeorectangleDef{
		TopLeft:     GeopositionDef{Latitude: northPoint.Latitude, Longitude: westPoint.Longitude},
		BottomRight: GeopositionDef{Latitude: southPoint.Latitude, Longitude: eastPoint.Longitude},
	}
}

// IntersectsRect returns true if "o" intersects r
func IntersectsRect(r GeorectangleDef, o GeorectangleDef) bool {
	return TopLeftOf(r.TopLeft, o.BottomRight) && BelowRightOf(r.BottomRight, o.TopLeft)
}

// ContainsRect returns true if "o" is within r
func ContainsRect(r GeorectangleDef, o GeorectangleDef) bool {
	return TopLeftOrSameOf(r.TopLeft, o.TopLeft) && BelowRightOrSameOf(r.BottomRight, o.BottomRight)
}

// ContainsPoint returns true if p is contained in r
func ContainsPoint(r GeorectangleDef, p GeopositionDef) bool {
	return TopLeftOrSameOf(r.TopLeft, p) && BelowRightOf(r.BottomRight, p)
}

// Split creates one new rectangle for each quadrant in "r"
func Split(r GeorectangleDef) (tlRect, blRect, trRect, brRect GeorectangleDef) {
	maxLat := r.TopLeft.Latitude
	minLon := r.TopLeft.Longitude
	minLat := r.BottomRight.Latitude
	maxLon := r.BottomRight.Longitude
	midLat := ((maxLat - minLat) / 2) + minLat
	midLon := ((maxLon - minLon) / 2) + minLon

	tlRect = GeorectangleDef{
		TopLeft:     GeopositionDef{Latitude: maxLat, Longitude: minLon},
		BottomRight: GeopositionDef{Latitude: midLat, Longitude: midLon},
	}
	blRect = GeorectangleDef{
		TopLeft:     GeopositionDef{Latitude: midLat, Longitude: minLon},
		BottomRight: GeopositionDef{Latitude: minLat, Longitude: midLon},
	}
	trRect = GeorectangleDef{
		TopLeft:     GeopositionDef{Latitude: maxLat, Longitude: midLon},
		BottomRight: GeopositionDef{Latitude: midLat, Longitude: maxLon},
	}
	brRect = GeorectangleDef{
		TopLeft:     GeopositionDef{Latitude: midLat, Longitude: midLon},
		BottomRight: GeopositionDef{Latitude: minLat, Longitude: maxLon},
	}

	return
}
