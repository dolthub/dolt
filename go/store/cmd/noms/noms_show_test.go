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

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
	"github.com/dolthub/dolt/go/store/util/test"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	clienttest.ClientTestSuite
}

const (
	res1 = "Commit{meta Struct,parents Set,parents_list List,value Ref} - struct Commit {\n  meta: struct {},\n  parents: set {},\n  parents_list: [],\n  value: #nl181uu1ioc2j6t7mt9paidjlhlcjtgj,\n}"
	res2 = "String - \"test string\""
	res3 = `Commit{meta Struct,parents Set,parents_closure Ref,parents_list List,value Ref} - struct Commit {
  meta: struct {},
  parents: set {
    #4u3mpdq0o8at437p37i5u94fk2frr4qm,
  },
  parents_closure: #pr2umfcqukd4ltrgkpvsjrig7afb9ghg,
  parents_list: [
    #4u3mpdq0o8at437p37i5u94fk2frr4qm,
  ],
  value: #t43ks6746hf0fcefv5e9v1c02k2i0jr9,
}`
	res4 = "List<Union<Float,String>> - [\n  \"elem1\",\n  2,\n  \"elem3\",\n]"
	res5 = `Commit{meta Struct,parents Set,parents_closure Ref,parents_list List,value Ref} - struct Commit {
  meta: struct {},
  parents: set {
    #idcre7pv1p74mfmidiguol1pu6rmt0bu,
  },
  parents_closure: #7pl4tlkc531difn1f32vlaqdve5g04p0,
  parents_list: [
    #idcre7pv1p74mfmidiguol1pu6rmt0bu,
  ],
  value: #nl181uu1ioc2j6t7mt9paidjlhlcjtgj,
}`
)

func (s *nomsShowTestSuite) spec(str string) spec.Spec {
	sp, err := spec.ForDataset(str)
	s.NoError(err)
	return sp
}
func (s *nomsShowTestSuite) writeTestData(str string, value types.Value) types.Ref {
	sp := s.spec(str)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())
	r1, err := vrw.WriteValue(context.Background(), value)
	s.NoError(err)
	_, err = datas.CommitValue(context.Background(), db, sp.GetDataset(context.Background()), r1)
	s.NoError(err)

	return r1
}

func (s *nomsShowTestSuite) TestNomsShow() {
	datasetName := "dsTest"
	str := spec.CreateValueSpecString("nbs", s.DBDir, datasetName)

	s1 := types.String("test string")
	r := s.writeTestData(str, s1)
	res, _ := s.MustRun(main, []string{"show", str})
	s.Equal(res1, res)

	str1 := spec.CreateValueSpecString("nbs", s.DBDir, "#"+r.TargetHash().String())
	res, _ = s.MustRun(main, []string{"show", str1})
	s.Equal(res2, res)

	sp := s.spec(str)
	defer sp.Close()
	list, err := types.NewList(context.Background(), sp.GetVRW(context.Background()), types.String("elem1"), types.Float(2), types.String("elem3"))
	s.NoError(err)
	r = s.writeTestData(str, list)
	res, _ = s.MustRun(main, []string{"show", str})
	test.EqualsIgnoreHashes(s.T(), res3, res)

	str1 = spec.CreateValueSpecString("nbs", s.DBDir, "#"+r.TargetHash().String())
	res, _ = s.MustRun(main, []string{"show", str1})
	s.Equal(res4, res)

	_ = s.writeTestData(str, s1)
	res, _ = s.MustRun(main, []string{"show", str})
	test.EqualsIgnoreHashes(s.T(), res5, res)
}

func (s *nomsShowTestSuite) TestNomsShowNotFound() {
	str := spec.CreateValueSpecString("nbs", s.DBDir, "not-there")
	stdout, stderr, err := s.Run(main, []string{"show", str})
	s.Equal("", stdout)
	s.Equal(fmt.Sprintf("Object not found: %s\n", str), stderr)
	s.Nil(err)
}

func (s *nomsShowTestSuite) TestNomsShowRaw() {
	datasetName := "showRaw"
	str := spec.CreateValueSpecString("nbs", s.DBDir, datasetName)
	sp, err := spec.ForDataset(str)
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase(context.Background())
	vrw := sp.GetVRW(context.Background())

	// Put a value into the db, get its raw serialization, then deserialize it and ensure it comes
	// out to same thing.
	test := func(in types.Value) {
		r1, err := vrw.WriteValue(context.Background(), in)
		s.NoError(err)
		datas.CommitValue(context.Background(), db, sp.GetDataset(context.Background()), r1)
		res, _ := s.MustRun(main, []string{"show", "--raw",
			spec.CreateValueSpecString("nbs", s.DBDir, "#"+r1.TargetHash().String())})
		ch := chunks.NewChunk([]byte(res))
		out, err := types.DecodeValue(ch, vrw)
		s.NoError(err)
		s.True(out.Equals(in))
	}

	// Primitive value with no child chunks
	test(types.String("hello"))

	// Ref (one child chunk)
	test(mustValue(vrw.WriteValue(context.Background(), types.Float(42))))

	// Prolly tree with multiple child chunks
	items := make([]types.Value, 10000)
	for i := 0; i < len(items); i++ {
		items[i] = types.Float(i)
	}
	l, err := types.NewList(context.Background(), vrw, items...)
	s.NoError(err)

	numChildChunks := 0
	_ = l.WalkRefs(vrw.Format(), func(r types.Ref) error {
		numChildChunks++
		return nil
	})
	s.True(numChildChunks > 0)
	test(l)
}
