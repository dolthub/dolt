// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"sort"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/stretchr/testify/suite"
)

func TestOpCache(t *testing.T) {
	suite.Run(t, &OpCacheSuite{})
}

type OpCacheSuite struct {
	suite.Suite
	vs *ValueStore
}

func (suite *OpCacheSuite) SetupTest() {
	suite.vs = newTestValueStore()
}

func (suite *OpCacheSuite) TearDownTest() {
	suite.vs.Close()
}

func (suite *OpCacheSuite) TestMapSet() {
	vs := suite.vs
	opCacheStore := newLdbOpCacheStore(vs)
	oc := opCacheStore.opCache()
	defer opCacheStore.destroy()

	entries := mapEntrySlice{
		{NewList(vs, Number(8), Number(0)), String("ahoy")},
		{String("A key"), NewBlob(vs, bytes.NewBufferString("A value"))},
		{Number(1), Bool(true)},
		{Bool(false), Number(1)},
		{NewBlob(vs, bytes.NewBuffer([]byte{0xff, 0, 0})), NewMap(vs)},
		{Bool(true), Number(42)},
		{NewStruct("thing1", StructData{"a": Number(7)}), Number(42)},
		{String("struct"), NewStruct("thing2", nil)},
		{Number(42), String("other")},
	}
	for _, entry := range entries {
		oc.GraphMapSet(nil, entry.key, entry.value)
	}
	sort.Sort(entries)

	iterated := mapEntrySlice{}
	iter := oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		keys, kind, item := iter.GraphOp()
		d.Chk.Empty(keys)
		d.Chk.Equal(MapKind, kind)
		iterated = append(iterated, item.(mapEntry))
	}
	suite.True(entries.Equals(iterated))
}

func (suite *OpCacheSuite) TestSetInsert() {
	vs := suite.vs
	opCacheStore := newLdbOpCacheStore(vs)
	oc := opCacheStore.opCache()
	defer opCacheStore.destroy()

	entries := ValueSlice{
		NewList(vs, Number(8), Number(0)),
		String("ahoy"),
		NewBlob(vs, bytes.NewBufferString("A value")),
		Number(1),
		Bool(true),
		Bool(false),
		NewBlob(vs, bytes.NewBuffer([]byte{0xff, 0, 0})),
		NewMap(vs),
		Number(42),
		NewStruct("thing1", StructData{"a": Number(7)}),
		String("struct"),
		NewStruct("thing2", nil),
		String("other"),
	}
	for _, entry := range entries {
		oc.GraphSetInsert(nil, entry)
	}
	sort.Sort(entries)

	iterated := ValueSlice{}
	iter := oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		keys, kind, item := iter.GraphOp()
		d.Chk.Empty(keys)
		d.Chk.Equal(SetKind, kind)
		iterated = append(iterated, item.(Value))
	}
	suite.True(entries.Equals(iterated))
}

func (suite *OpCacheSuite) TestListAppend() {
	vs := suite.vs
	opCacheStore := newLdbOpCacheStore(vs)
	oc := opCacheStore.opCache()
	defer opCacheStore.destroy()

	entries := ValueSlice{
		NewList(vs, Number(8), Number(0)),
		String("ahoy"),
		NewBlob(vs, bytes.NewBufferString("A value")),
		Number(1),
		Bool(true),
		Bool(false),
		NewBlob(vs, bytes.NewBuffer([]byte{0xff, 0, 0})),
		NewMap(vs),
		Number(42),
		NewStruct("thing1", StructData{"a": Number(7)}),
		String("struct"),
		NewStruct("thing2", nil),
		String("other"),
	}
	for _, entry := range entries {
		oc.GraphListAppend(nil, entry)
	}

	iterated := ValueSlice{}
	iter := oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		keys, kind, item := iter.GraphOp()
		d.Chk.Empty(keys)
		d.Chk.Equal(ListKind, kind)
		iterated = append(iterated, item.(Value))
	}
	suite.True(entries.Equals(iterated))
}
