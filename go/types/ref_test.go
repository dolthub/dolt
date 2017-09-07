// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefInList(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	l := NewList(vs)
	r := NewRef(l)
	l = l.Edit().Append(r).List()
	r2 := l.Get(0)
	assert.True(r.Equals(r2))
}

func TestRefInSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	s := NewSet(vs)
	r := NewRef(s)
	s = s.Edit().Insert(r).Set()
	r2 := s.First()
	assert.True(r.Equals(r2))
}

func TestRefInMap(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	m := NewMap(vs)
	r := NewRef(m)
	m = m.Edit().Set(Number(0), r).Set(r, Number(1)).Map()
	r2 := m.Get(Number(0))
	assert.True(r.Equals(r2))

	i := m.Get(r)
	assert.Equal(int32(1), int32(i.(Number)))
}

func TestRefChunks(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	l := NewList(vs)
	r := NewRef(l)
	assert.Len(getChunks(r), 1)
	assert.Equal(r, getChunks(r)[0])
}
