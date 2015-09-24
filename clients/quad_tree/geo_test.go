package main

import (
	"fmt"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"testing"
)

func TestPoint(t *testing.T) {
	assert := assert.New(t)
	p1 := GeopositionDef{5, 5}
	p2 := GeopositionDef{3, 7}
	p3 := GeopositionDef{10, 7}
	p4 := GeopositionDef{10, 12}

	assert.True(p1.TopLeftOf(p2))
	assert.False(p2.TopLeftOf(p1))
	assert.True(p2.BelowRightOf(p1))
	assert.False(p1.BelowRightOf(p2))
	assert.False(p1.TopLeftOf(p1))
	assert.False(p2.BelowRightOf(p2))

	assert.True(p3.TopLeftOrSameOf(p2))
	assert.True(p3.TopLeftOrSameOf(p3))
	assert.False(p4.TopLeftOrSameOf(p3))
	assert.True(p2.BelowRightOrSameOf(p3))
	assert.True(p3.BelowRightOrSameOf(p3))
	assert.False(p3.BelowRightOrSameOf(p4))
	assert.True(p3.TopLeftOrSameOf(p4))
}

func TestRectangle(t *testing.T) {
	assert := assert.New(t)
	tl1 := GeopositionDef{10, 5}
	br1 := GeopositionDef{5, 10}
	r1 := GeorectangleDef{tl1, br1}

	tl2 := GeopositionDef{9, 7}
	br2 := GeopositionDef{7, 9}
	r2 := GeorectangleDef{tl2, br2}

	tl3 := GeopositionDef{30, 20}
	br3 := GeopositionDef{20, 30}
	r3 := GeorectangleDef{tl3, br3}

	assert.True(tl1.TopLeftOf(br2))

	assert.True(r1.IntersectsRect(r2))
	assert.True(r2.IntersectsRect(r1))
	assert.True(r2.IntersectsRect(r2))

	assert.True(r1.ContainsRect(r2))
	assert.False(r2.ContainsRect(r1))
	assert.True(r2.ContainsRect(r2))

	assert.False(r1.IntersectsRect(r3))
	assert.False(r1.ContainsRect(r3))

	assert.True(r1.ContainsPoint(tl2))
	assert.False(r1.ContainsPoint(br3))

	tlRect, blRect, trRect, brRect := r3.Split()
	tl0 := GeopositionDef{30, 20}
	tm0 := GeopositionDef{30, 25}
	lm0 := GeopositionDef{25, 20}
	mm0 := GeopositionDef{25, 25}
	rm0 := GeopositionDef{25, 30}
	bm0 := GeopositionDef{20, 25}
	br0 := GeopositionDef{20, 30}

	topLeft := GeorectangleDef{tl0, mm0}
	bottomLeft := GeorectangleDef{lm0, bm0}
	topRight := GeorectangleDef{tm0, rm0}
	bottomRight := GeorectangleDef{mm0, br0}

	assert.True(tlRect == topLeft)
	assert.True(blRect == bottomLeft)
	assert.True(trRect == topRight)
	assert.True(brRect == bottomRight)
}

func TestDistances(t *testing.T) {
	p1 := GeopositionDef{37.83, -122.52}
	p2 := GeopositionDef{37.70, -122.36}

	km1 := p1.DistanceTo(p2)
	fmt.Printf("distance from %v to %v: %f\n", p1, p2, km1)
	km2 := p2.DistanceTo(p1)
	fmt.Printf("distance from %v to %v: %f\n", p2, p1, km2)
	assert.Equal(t, km1, km2)

	point := GeopositionDef{37.7644008, -122.4511607}
	kms := 0.9
	boundingRect := point.BoundingRectangle(kms)
	fmt.Printf("Point: %v, distance: %f kms, Bounding %v", point, kms, boundingRect)
}
