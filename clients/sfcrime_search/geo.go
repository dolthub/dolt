package main

import "math"

const (
	// According to Wikipedia, the Earth's radius is about 6,371km
	EARTH_RADIUS = 6371
)

func (p GeopositionDef) Latitude64() float64 {
	return float64(p.Latitude)
}

func (p GeopositionDef) Longitude64() float64 {
	return float64(p.Longitude)
}

// LessThan returns true if it is above and to the left of "o"
func (p GeopositionDef) TopLeftOf(o GeopositionDef) bool {
	return p.Latitude > o.Latitude && p.Longitude < o.Longitude
}

// GreaterThan returns true if it's below and to the right of "o"
func (p GeopositionDef) BelowRightOf(o GeopositionDef) bool {
	return p.Latitude < o.Latitude && p.Longitude > o.Longitude
}

// LessThanOrEqual returns true if it is above and to the left of "o"
func (p GeopositionDef) TopLeftOrSameOf(o GeopositionDef) bool {
	return p.Latitude >= o.Latitude && p.Longitude <= o.Longitude
}

// GreaterThanOrEqual returns true if it's below and to the right of "o"
func (p GeopositionDef) BelowRightOrSameOf(o GeopositionDef) bool {
	return p.Latitude <= o.Latitude && p.Longitude >= o.Longitude
}

// Returns a Point populated with the lat and lng coordinates by transposing the origin point the passed in distance (in kilometers) by the passed in compass bearing (in degrees). Original Implementation from: http://www.movable-type.co.uk/scripts/latlong.html
func (p *GeopositionDef) PointAtDistanceAndBearing(dist float64, bearing float64) *GeopositionDef {
	dr := dist / EARTH_RADIUS

	bearing = (bearing * (math.Pi / 180.0))

	lat1 := (float64(p.Latitude64()) * (math.Pi / 180.0))
	lng1 := (float64(p.Longitude64()) * (math.Pi / 180.0))

	lat2_part1 := math.Sin(lat1) * math.Cos(dr)
	lat2_part2 := math.Cos(lat1) * math.Sin(dr) * math.Cos(bearing)

	lat2 := math.Asin(lat2_part1 + lat2_part2)

	lng2_part1 := math.Sin(bearing) * math.Sin(dr) * math.Cos(lat1)
	lng2_part2 := math.Cos(dr) - (math.Sin(lat1) * math.Sin(lat2))

	lng2 := lng1 + math.Atan2(lng2_part1, lng2_part2)
	lng2 = math.Mod((lng2+3*math.Pi), (2*math.Pi)) - math.Pi

	lat2 = lat2 * (180.0 / math.Pi)
	lng2 = lng2 * (180.0 / math.Pi)

	return &GeopositionDef{Latitude: float32(lat2), Longitude: float32(lng2)}
}

// Calculates the Haversine distance between two points in kilometers. Original Implementation from: http://www.movable-type.co.uk/scripts/latlong.html
func (p GeopositionDef) DistanceTo(o GeopositionDef) float32 {
	dLat := (o.Latitude64() - p.Latitude64()) * (math.Pi / 180.0)
	dLon := (o.Longitude64() - p.Longitude64()) * (math.Pi / 180.0)

	lat1 := p.Latitude64() * (math.Pi / 180.0)
	lat2 := o.Latitude64() * (math.Pi / 180.0)

	a1 := math.Sin(dLat/2) * math.Sin(dLat/2)
	a2 := math.Sin(dLon/2) * math.Sin(dLon/2) * math.Cos(lat1) * math.Cos(lat2)

	a := a1 + a2

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return float32(EARTH_RADIUS * c)
}

func (p GeopositionDef) BoundingRectangle(radius float64) GeorectangleDef {
	northPoint := p.PointAtDistanceAndBearing(radius, 0)
	eastPoint := p.PointAtDistanceAndBearing(radius, 90)
	southPoint := p.PointAtDistanceAndBearing(radius, 180)
	westPoint := p.PointAtDistanceAndBearing(radius, 270)

	return GeorectangleDef{
		TopLeft:     GeopositionDef{northPoint.Latitude, westPoint.Longitude},
		BottomRight: GeopositionDef{southPoint.Latitude, eastPoint.Longitude},
	}
}

// IntersectsRect returns true if "o" intersects r
func (r GeorectangleDef) IntersectsRect(o GeorectangleDef) bool {
	return r.TopLeft.TopLeftOf(o.BottomRight) && r.BottomRight.BelowRightOf(o.TopLeft)
}

// ContainsRect returns true if "o" is within r
func (r GeorectangleDef) ContainsRect(o GeorectangleDef) bool {
	return r.TopLeft.TopLeftOrSameOf(o.TopLeft) && r.BottomRight.BelowRightOrSameOf(o.BottomRight)
}

// ContainsPoint returns true if p is contained in r
func (r GeorectangleDef) ContainsPoint(p GeopositionDef) bool {
	return r.TopLeft.TopLeftOrSameOf(p) && r.BottomRight.BelowRightOf(p)
}

// Split creates one new rectangle for each quadrant in "r"
func (r GeorectangleDef) Split() (tlRect, blRect, trRect, brRect GeorectangleDef) {
	maxLat := float32(r.TopLeft.Latitude)
	minLon := float32(r.TopLeft.Longitude)
	minLat := float32(r.BottomRight.Latitude)
	maxLon := float32(r.BottomRight.Longitude)
	midLat := ((maxLat - minLat) / 2) + minLat
	midLon := ((maxLon - minLon) / 2) + minLon

	tlRect = GeorectangleDef{GeopositionDef{maxLat, minLon}, GeopositionDef{midLat, midLon}}
	blRect = GeorectangleDef{GeopositionDef{midLat, minLon}, GeopositionDef{minLat, midLon}}
	trRect = GeorectangleDef{GeopositionDef{maxLat, midLon}, GeopositionDef{midLat, maxLon}}
	brRect = GeorectangleDef{GeopositionDef{midLat, midLon}, GeopositionDef{minLat, maxLon}}

	return
}
