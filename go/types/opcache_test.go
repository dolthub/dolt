// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"sort"
	"testing"

	"github.com/attic-labs/testify/suite"
)

func TestOpCache(t *testing.T) {
	suite.Run(t, &OpCacheSuite{})
}

type OpCacheSuite struct {
	suite.Suite
	vs *ValueStore
	oc *opCache
}

func (suite *OpCacheSuite) SetupTest() {
	suite.vs = NewTestValueStore()
	suite.oc = newOpCache(suite.vs)
}

func (suite *OpCacheSuite) TearDownTest() {
	suite.vs.Close()
	suite.oc.Destroy()
}

func (suite *OpCacheSuite) TestSet() {
	entries := mapEntrySlice{
		{NewList(Number(8), Number(0)), String("ahoy")},
		{String("A key"), NewBlob(bytes.NewBufferString("A value"))},
		{Number(1), Bool(true)},
		{Bool(false), Number(1)},
		{NewBlob(bytes.NewBuffer([]byte{0xff, 0, 0})), NewMap()},
		{Bool(true), Number(42)},
		{NewStruct("thing1", structData{"a": Number(7)}), Number(42)},
		{String("struct"), NewStruct("thing2", nil)},
		{Number(42), String("other")},
	}
	for _, entry := range entries {
		suite.oc.Set(entry.key, entry.value)
	}
	sort.Sort(entries)

	iterated := mapEntrySlice{}
	iter := suite.oc.NewIterator()
	defer iter.Release()
	for iter.Next() {
		iterated = append(iterated, iter.Op().(mapEntry))
	}
	suite.True(entries.Equals(iterated))
}
