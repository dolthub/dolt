// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
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
		{NewList(context.Background(), vs, Float(8), Float(0)), String("ahoy")},
		{String("A key"), NewBlob(context.Background(), vs, bytes.NewBufferString("A value"))},
		{Float(1), Bool(true)},
		{Bool(false), Float(1)},
		{NewBlob(context.Background(), vs, bytes.NewBuffer([]byte{0xff, 0, 0})), NewMap(context.Background(), vs)},
		{Bool(true), Float(42)},
		{NewStruct("thing1", StructData{"a": Float(7)}), Float(42)},
		{String("struct"), NewStruct("thing2", nil)},
		{Float(42), String("other")},
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
		NewList(context.Background(), vs, Float(8), Float(0)),
		String("ahoy"),
		NewBlob(context.Background(), vs, bytes.NewBufferString("A value")),
		Float(1),
		Bool(true),
		Bool(false),
		NewBlob(context.Background(), vs, bytes.NewBuffer([]byte{0xff, 0, 0})),
		NewMap(context.Background(), vs),
		Float(42),
		NewStruct("thing1", StructData{"a": Float(7)}),
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
		NewList(context.Background(), vs, Float(8), Float(0)),
		String("ahoy"),
		NewBlob(context.Background(), vs, bytes.NewBufferString("A value")),
		Float(1),
		Bool(true),
		Bool(false),
		NewBlob(context.Background(), vs, bytes.NewBuffer([]byte{0xff, 0, 0})),
		NewMap(context.Background(), vs),
		Float(42),
		NewStruct("thing1", StructData{"a": Float(7)}),
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
