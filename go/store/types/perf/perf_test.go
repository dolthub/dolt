// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package perf

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/perf/suite"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
)

type perfSuite struct {
	suite.PerfSuite
	r *rand.Rand
}

func (s *perfSuite) SetupSuite() {
	s.r = rand.New(rand.NewSource(0))
}

func (s *perfSuite) Test01BuildList10mNumbers() {
	assert := s.NewAssert()
	in := make(chan types.Value, 16)
	out := types.NewStreamingList(context.Background(), types.Format_7_18, s.Database, in)

	for i := 0; i < 1e7; i++ {
		in <- types.Float(s.r.Int63())
	}
	close(in)

	ds := s.Database.GetDataset(context.Background(), "BuildList10mNumbers")

	var err error
	ds, err = s.Database.CommitValue(context.Background(), ds, <-out)

	assert.NoError(err)
	s.Database = ds.Database()
}

func (s *perfSuite) Test02BuildList10mStructs() {
	assert := s.NewAssert()
	in := make(chan types.Value, 16)
	out := types.NewStreamingList(context.Background(), types.Format_7_18, s.Database, in)

	for i := 0; i < 1e7; i++ {
		in <- types.NewStruct("", types.StructData{
			"number": types.Float(s.r.Int63()),
		})
	}
	close(in)

	ds := s.Database.GetDataset(context.Background(), "BuildList10mStructs")

	var err error
	ds, err = s.Database.CommitValue(context.Background(), ds, <-out)

	assert.NoError(err)
	s.Database = ds.Database()
}

func (s *perfSuite) Test03Read10mNumbers() {
	s.headList("BuildList10mNumbers").IterAll(context.Background(), func(v types.Value, index uint64) {})
}

func (s *perfSuite) Test04Read10mStructs() {
	s.headList("BuildList10mStructs").IterAll(context.Background(), func(v types.Value, index uint64) {})
}

func (s *perfSuite) Test05Concat10mValues2kTimes() {
	assert := s.NewAssert()

	last := func(v types.List) types.Value {
		return v.Get(context.Background(), v.Len()-1)
	}

	l1 := s.headList("BuildList10mNumbers")
	l2 := s.headList("BuildList10mStructs")
	l1Len, l2Len := l1.Len(), l2.Len()
	l1Last, l2Last := last(l1), last(l2)

	l3 := types.NewList(context.Background(), types.Format_7_18, s.Database)
	for i := uint64(0); i < 1e3; i++ { // 1k iterations * 2 concat ops = 2k times
		// Include some basic sanity checks.
		l3 = l3.Concat(context.Background(), types.Format_7_18, l1)
		assert.True(l1Last.Equals(last(l3)))
		assert.Equal(i*(l1Len+l2Len)+l1Len, l3.Len())
		l3 = l3.Concat(context.Background(), types.Format_7_18, l2)
		assert.True(l2Last.Equals(last(l3)))
		assert.Equal((i+1)*(l1Len+l2Len), l3.Len())
	}

	ds := s.Database.GetDataset(context.Background(), "Concat10mValues2kTimes")
	var err error
	ds, err = s.Database.CommitValue(context.Background(), ds, l3)

	assert.NoError(err)
	s.Database = ds.Database()
}

func (s *perfSuite) TestBuild500megBlobFromFilesP1() {
	s.testBuild500megBlob(1)
}

func (s *perfSuite) TestBuild500megBlobFromFilesP2() {
	s.testBuild500megBlob(2)
}

func (s *perfSuite) TestBuild500megBlobFromFilesP8() {
	s.testBuild500megBlob(8)
}

func (s *perfSuite) TestBuild500megBlobFromFilesP64() {
	// Note: can't have too many files open.
	s.testBuild500megBlob(64)
}

func (s *perfSuite) testBuild500megBlob(p int) {
	assert := s.NewAssert()
	size := int(5e8)

	readers := make([]io.Reader, p)
	defer func() {
		for _, r := range readers {
			f := r.(*os.File)
			err := f.Close()
			assert.NoError(err)
			err = os.Remove(f.Name())
			assert.NoError(err)
		}
	}()

	s.Pause(func() {
		for i := range readers {
			f, err := ioutil.TempFile("", "testBuildBlob")
			assert.NoError(err)
			_, err = f.Write(s.randomBytes(int64(i), size/p))
			assert.NoError(err)
			err = f.Close()
			assert.NoError(err)
			f, err = os.Open(f.Name())
			assert.NoError(err)
			readers[i] = f
		}
	})

	// TODO(binformat)
	b := types.NewBlob(context.Background(), types.Format_7_18, s.Database, readers...)
	assert.Equal(uint64(size), b.Len())
}

func (s *perfSuite) randomBytes(seed int64, size int) []byte {
	r := rand.New(rand.NewSource(seed))
	randBytes := make([]byte, size)
	_, err := r.Read(randBytes)
	assert.NoError(s.T, err)
	return randBytes
}

func (s *perfSuite) headList(dsName string) types.List {
	ds := s.Database.GetDataset(context.Background(), dsName)
	return ds.HeadValue().(types.List)
}

func TestPerf(t *testing.T) {
	suite.Run("types", t, &perfSuite{})
}
