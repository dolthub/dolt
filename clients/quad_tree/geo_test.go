package main

import (
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"testing"
)

func TestPoint(t *testing.T) {
	assert := assert.New(t)
	p1 := NewGeoposition().SetLongitude(5).SetLatitude(5)
	p2 := NewGeoposition().SetLongitude(7).SetLatitude(3)
	p3 := NewGeoposition().SetLongitude(7).SetLatitude(10)
	p4 := NewGeoposition().SetLongitude(12).SetLatitude(10)

	assert.True(p1.LessThan(p2))
	assert.False(p2.LessThan(p1))
	assert.True(p2.GreaterThan(p1))
	assert.False(p1.GreaterThan(p2))
	assert.False(p1.LessThan(p1))
	assert.False(p2.GreaterThan(p2))

	assert.True(p3.LessThanOrEqual(p2))
	assert.True(p3.LessThanOrEqual(p3))
	assert.False(p4.LessThanOrEqual(p3))
	assert.True(p2.GreaterThanOrEqual(p3))
	assert.True(p3.GreaterThanOrEqual(p3))
	assert.False(p3.GreaterThanOrEqual(p4))
	assert.True(p3.LessThanOrEqual(p4))
}

func TestRectangle(t *testing.T) {
	assert := assert.New(t)
	tl1 := NewGeoposition().SetLongitude(5.0).SetLatitude(10.0)
	br1 := NewGeoposition().SetLongitude(10.0).SetLatitude(5.0)
	r1 := NewGeorectangle().SetTopLeft(tl1).SetBottomRight(br1)

	tl2 := NewGeoposition().SetLongitude(7.0).SetLatitude(9.0)
	br2 := NewGeoposition().SetLongitude(9.0).SetLatitude(7.0)
	r2 := NewGeorectangle().SetTopLeft(tl2).SetBottomRight(br2)

	tl3 := NewGeoposition().SetLongitude(20.0).SetLatitude(30.0)
	br3 := NewGeoposition().SetLongitude(30.0).SetLatitude(20.0)
	r3 := NewGeorectangle().SetTopLeft(tl3).SetBottomRight(br3)

	assert.True(tl1.LessThan(br2))

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

	rects := r3.Split()
	assert.Equal(4, len(rects))
	tl0 := NewGeoposition().SetLongitude(20.0).SetLatitude(30.0)
	tm0 := NewGeoposition().SetLongitude(25.0).SetLatitude(30.0)
	lm0 := NewGeoposition().SetLongitude(20.0).SetLatitude(25.0)
	mm0 := NewGeoposition().SetLongitude(25.0).SetLatitude(25.0)
	rm0 := NewGeoposition().SetLongitude(30.0).SetLatitude(25.0)
	bm0 := NewGeoposition().SetLongitude(25.0).SetLatitude(20.0)
	br0 := NewGeoposition().SetLongitude(30.0).SetLatitude(20.0)

	topLeft := NewGeorectangle().SetTopLeft(tl0).SetBottomRight(mm0)
	bottomLeft := NewGeorectangle().SetTopLeft(lm0).SetBottomRight(bm0)
	topRight := NewGeorectangle().SetTopLeft(tm0).SetBottomRight(rm0)
	bottomRight := NewGeorectangle().SetTopLeft(mm0).SetBottomRight(br0)

	assert.True(rects[tl].Equals(topLeft))
	assert.True(rects[bl].Equals(bottomLeft))
	assert.True(rects[tr].Equals(topRight))
	assert.True(rects[br].Equals(bottomRight))
}
