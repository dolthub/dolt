// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package perf

import (
	"context"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/perf/suite"
	"github.com/dolthub/dolt/go/store/types"
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
	ae := atomicerr.New()
	out := types.NewStreamingList(context.Background(), s.VS, ae, in)

	for i := 0; i < 1e7; i++ {
		in <- types.Float(s.r.Int63())
	}
	close(in)

	ds, err := s.Database.GetDataset(context.Background(), "BuildList10mNumbers")
	assert.NoError(err)

	ds, err = datas.CommitValue(context.Background(), s.Database, ds, <-out)
	assert.NoError(err)

	assert.NoError(ae.Get())
	s.Database = ds.Database()
}

func (s *perfSuite) Test02BuildList10mStructs() {
	assert := s.NewAssert()
	in := make(chan types.Value, 16)
	ae := atomicerr.New()
	out := types.NewStreamingList(context.Background(), s.VS, ae, in)

	for i := 0; i < 1e7; i++ {
		st, err := types.NewStruct(types.Format_7_18, "", types.StructData{
			"number": types.Float(s.r.Int63()),
		})

		assert.NoError(err)
		in <- st
	}
	close(in)

	ds, err := s.Database.GetDataset(context.Background(), "BuildList10mStructs")
	assert.NoError(err)

	ds, err = datas.CommitValue(context.Background(), s.Database, ds, <-out)
	assert.NoError(err)

	assert.NoError(ae.Get())
	s.Database = ds.Database()
}

func (s *perfSuite) Test03Read10mNumbers() {
	s.headList("BuildList10mNumbers").IterAll(context.Background(), func(v types.Value, index uint64) error { return nil })
}

func (s *perfSuite) Test04Read10mStructs() {
	s.headList("BuildList10mStructs").IterAll(context.Background(), func(v types.Value, index uint64) error { return nil })
}

func (s *perfSuite) Test05Concat10mValues2kTimes() {
	assert := s.NewAssert()

	last := func(v types.List) types.Value {
		res, err := v.Get(context.Background(), v.Len()-1)
		assert.NoError(err)
		return res
	}

	l1 := s.headList("BuildList10mNumbers")
	l2 := s.headList("BuildList10mStructs")
	l1Len, l2Len := l1.Len(), l2.Len()
	l1Last, l2Last := last(l1), last(l2)

	l3, err := types.NewList(context.Background(), s.VS)
	assert.NoError(err)
	for i := uint64(0); i < 1e3; i++ { // 1k iterations * 2 concat ops = 2k times
		// Include some basic sanity checks.
		l3, err = l3.Concat(context.Background(), l1)
		assert.NoError(err)
		assert.True(l1Last.Equals(last(l3)))
		assert.Equal(i*(l1Len+l2Len)+l1Len, l3.Len())
		l3, err = l3.Concat(context.Background(), l2)
		assert.NoError(err)
		assert.True(l2Last.Equals(last(l3)))
		assert.Equal((i+1)*(l1Len+l2Len), l3.Len())
	}

	ds, err := s.Database.GetDataset(context.Background(), "Concat10mValues2kTimes")
	assert.NoError(err)

	ds, err = datas.CommitValue(context.Background(), s.Database, ds, l3)
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
			err = file.Remove(f.Name())
			assert.NoError(err)
		}
	}()

	s.Pause(func() {
		for i := range readers {
			f, err := os.CreateTemp("", "testBuildBlob")
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

	b, err := types.NewBlob(context.Background(), s.VS, readers...)
	assert.NoError(err)
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
	ass := s.NewAssert()
	ds, err := s.Database.GetDataset(context.Background(), dsName)
	ass.NoError(err)
	headVal, ok, err := ds.MaybeHeadValue()
	ass.NoError(err)
	ass.True(ok)
	return headVal.(types.List)
}

func TestPerf(t *testing.T) {
	suite.Run("types", t, &perfSuite{})
}
