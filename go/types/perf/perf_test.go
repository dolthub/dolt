// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package perf

import (
	"math/rand"
	"testing"

	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/perf/suite"
	"github.com/attic-labs/noms/go/types"
)

type perfSuite struct {
	suite.PerfSuite
	r  *rand.Rand
	ds string
}

func (s *perfSuite) SetupSuite() {
	s.r = rand.New(rand.NewSource(0))
}

func (s *perfSuite) Test01BuildList10mNumbers() {
	assert := s.NewAssert()
	in := make(chan types.Value, 16)
	out := types.NewStreamingList(s.Database, in)

	for i := 0; i < 1e7; i++ {
		in <- types.Number(s.r.Int63())
	}
	close(in)

	ds := dataset.NewDataset(s.Database, "BuildList10mNumbers")

	var err error
	ds, err = ds.CommitValue(<-out)

	assert.NoError(err)
	s.Database = ds.Database()
}

func (s *perfSuite) Test02BuildList10mStructs() {
	assert := s.NewAssert()
	in := make(chan types.Value, 16)
	out := types.NewStreamingList(s.Database, in)

	for i := 0; i < 1e7; i++ {
		in <- types.NewStruct("", types.StructData{
			"number": types.Number(s.r.Int63()),
		})
	}
	close(in)

	ds := dataset.NewDataset(s.Database, "BuildList10mStructs")

	var err error
	ds, err = ds.CommitValue(<-out)

	assert.NoError(err)
	s.Database = ds.Database()
}

func (s *perfSuite) Test03Read10mNumbers() {
	s.headList("BuildList10mNumbers").IterAll(func(v types.Value, index uint64) {})
}

func (s *perfSuite) Test04Read10mStructs() {
	s.headList("BuildList10mStructs").IterAll(func(v types.Value, index uint64) {})
}

func (s *perfSuite) Test05Concat10mValues2kTimes() {
	assert := s.NewAssert()

	last := func(v types.List) types.Value {
		return v.Get(v.Len() - 1)
	}

	l1 := s.headList("BuildList10mNumbers")
	l2 := s.headList("BuildList10mStructs")
	l1Len, l2Len := l1.Len(), l2.Len()
	l1Last, l2Last := last(l1), last(l2)

	l3 := types.NewList()
	for i := uint64(0); i < 1e3; i++ { // 1k iterations * 2 concat ops = 2k times
		// Include some basic sanity checks.
		l3 = l3.Concat(l1)
		assert.True(l1Last.Equals(last(l3)))
		assert.Equal(i*(l1Len+l2Len)+l1Len, l3.Len())
		l3 = l3.Concat(l2)
		assert.True(l2Last.Equals(last(l3)))
		assert.Equal((i+1)*(l1Len+l2Len), l3.Len())
	}

	ds := dataset.NewDataset(s.Database, "Concat10mValues2kTimes")
	var err error
	ds, err = ds.CommitValue(l3)

	assert.NoError(err)
	s.Database = ds.Database()
}

func (s *perfSuite) headList(dsName string) types.List {
	ds := dataset.NewDataset(s.Database, dsName)
	return ds.HeadValue().(types.List)
}

func TestPerf(t *testing.T) {
	suite.Run("types", t, &perfSuite{})
}
