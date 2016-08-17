// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

const nilHolder = -1000000

var (
	r1  = qr(2, true, 5, true)
	r2  = qr(0, true, 8, true)
	r3  = qr(0, true, 3, true)
	r4  = qr(3, true, 8, true)
	r5  = qr(0, true, 1, true)
	r6  = qr(6, true, 10, true)
	r7  = qr(nilHolder, true, 10, true)
	r8  = qr(3, true, nilHolder, true)
	r10 = qr(2, true, 5, false)
	r11 = qr(5, true, 10, true)
)

func newBound(i int, include bool, infinity int) bound {
	var v types.Value
	if i != nilHolder {
		v = types.Number(i)
	}
	return bound{value: v, include: include, infinity: int8(infinity)}
}

func qr(lower int, lowerIncl bool, upper int, upperIncl bool) queryRange {
	lowerInf := 0
	if lower == nilHolder {
		lowerInf = -1
	}
	upperInf := 0
	if upper == nilHolder {
		upperInf = 1
	}
	return queryRange{newBound(lower, lowerIncl, lowerInf), newBound(upper, upperIncl, upperInf)}
}

func TestRangeIntersects(t *testing.T) {
	assert := assert.New(t)

	assert.True(r1.intersects(r2))
	assert.True(r1.intersects(r3))
	assert.True(r1.intersects(r4))
	assert.True(r2.intersects(r1))
	assert.True(r1.intersects(r7))
	assert.True(r1.intersects(r8))
	assert.True(r3.intersects(r4))
	assert.True(r3.intersects(r4))

	assert.False(r1.intersects(r5))
	assert.False(r1.intersects(r6))
	assert.False(r10.intersects(r11))
}

func TestRangeAnd(t *testing.T) {
	assert := assert.New(t)

	assert.Empty(r1.and(r5))
	assert.Empty(r1.and(r6))

	assert.Equal(r1, r1.and(r2)[0])
	assert.Equal(r1, r2.and(r1)[0])

	expected := qr(3, true, 5, true)
	assert.Equal(expected, r1.and(r4)[0])
}

func TestRangeOr(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(r2, r1.or(r2)[0])

	expected := qr(0, true, 5, true)
	assert.Equal(expected, r1.or(r3)[0])

	expectedSlice := queryRangeSlice{r5, r1}
	assert.Equal(expectedSlice, r1.or(r5))
	assert.Equal(expectedSlice, r5.or(r1))
}

func TestIsLessThan(t *testing.T) {
	assert := assert.New(t)

	assert.True(newBound(1, true, 0).isLessThanOrEqual(newBound(2, true, 0)))
	assert.False(newBound(2, true, 0).isLessThanOrEqual(newBound(1, true, 0)))
	assert.True(newBound(1, true, 0).isLessThanOrEqual(newBound(1, true, 0)))

	assert.True(newBound(1, false, 0).isLessThanOrEqual(newBound(2, false, 0)))
	assert.False(newBound(2, false, 0).isLessThanOrEqual(newBound(1, false, 0)))
	assert.True(newBound(1, false, 0).isLessThanOrEqual(newBound(1, false, 0)))

	assert.False(newBound(1, true, 0).isLessThanOrEqual(newBound(1, false, 0)))
	assert.True(newBound(1, false, 0).isLessThanOrEqual(newBound(1, true, 0)))

	assert.True(newBound(nilHolder, true, -1).isLessThanOrEqual(newBound(1, true, 0)))
	assert.False(newBound(1, false, 0).isLessThanOrEqual(newBound(nilHolder, true, -1)))
}

func TestIsGreaterThan(t *testing.T) {
	assert := assert.New(t)

	assert.True(newBound(2, true, 0).isGreaterThanOrEqual(newBound(1, true, 0)))
	assert.False(newBound(1, true, 0).isGreaterThanOrEqual(newBound(2, true, 0)))
	assert.True(newBound(1, true, 0).isGreaterThanOrEqual(newBound(1, true, 0)))

	assert.True(newBound(2, false, 0).isGreaterThanOrEqual(newBound(1, false, 0)))
	assert.False(newBound(1, false, 0).isGreaterThanOrEqual(newBound(2, false, 0)))
	assert.True(newBound(1, false, 0).isGreaterThanOrEqual(newBound(1, false, 0)))

	assert.True(newBound(1, true, 0).isGreaterThanOrEqual(newBound(1, false, 0)))
	assert.False(newBound(1, false, 0).isGreaterThanOrEqual(newBound(2, true, 0)))

	assert.True(newBound(nilHolder, true, 1).isGreaterThanOrEqual(newBound(1, true, 0)))
	assert.False(newBound(1, true, 0).isGreaterThanOrEqual(newBound(nilHolder, true, 1)))
}

func TestMinValue(t *testing.T) {
	assert := assert.New(t)
	ve1 := newBound(5, false, 0)
	ve2 := newBound(5, true, 0)
	ve3 := newBound(nilHolder, true, -1)
	ve4 := newBound(nilHolder, true, 1)

	assert.Equal(ve1, ve1.minValue(ve2))
	assert.Equal(ve3, ve1.minValue(ve3))
	assert.Equal(ve1, ve1.minValue(ve4))
}

func TestMaxValue(t *testing.T) {
	assert := assert.New(t)
	ve1 := newBound(5, false, 0)
	ve2 := newBound(5, true, 0)
	ve3 := newBound(nilHolder, true, -1)
	ve4 := newBound(nilHolder, true, 1)

	assert.Equal(ve2, ve1.maxValue(ve2))
	assert.Equal(ve1, ve1.maxValue(ve3))
	assert.Equal(ve4, ve1.maxValue(ve4))
}
