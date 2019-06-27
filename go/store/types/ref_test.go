// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefInList(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	// TODO(binformat)
	l := NewList(context.Background(), Format_7_18, vs)
	r := NewRef(l)
	l = l.Edit().Append(r).List(context.Background())
	r2 := l.Get(context.Background(), 0)
	assert.True(r.Equals(r2))
}

func TestRefInSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	s := NewSet(context.Background(), vs)
	r := NewRef(s)
	s = s.Edit().Insert(r).Set(context.Background())
	r2 := s.First(context.Background())
	assert.True(r.Equals(r2))
}

func TestRefInMap(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	m := NewMap(context.Background(), vs)
	r := NewRef(m)
	m = m.Edit().Set(Float(0), r).Set(r, Float(1)).Map(context.Background())
	r2 := m.Get(context.Background(), Float(0))
	assert.True(r.Equals(r2))

	i := m.Get(context.Background(), r)
	assert.Equal(int32(1), int32(i.(Float)))
}

func TestRefChunks(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	// TODO(binformat)
	l := NewList(context.Background(), Format_7_18, vs)
	r := NewRef(l)
	assert.Len(getChunks(r), 1)
	assert.Equal(r, getChunks(r)[0])
}
