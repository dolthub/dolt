// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/spec"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/liquidata-inc/dolt/go/store/util/clienttest"
	"github.com/liquidata-inc/dolt/go/store/util/test"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	clienttest.ClientTestSuite
}

const (
	res1 = "struct Commit {\n  meta: struct {},\n  parents: set {},\n  parents_list: [],\n  value: #nl181uu1ioc2j6t7mt9paidjlhlcjtgj,\n}"
	res2 = "\"test string\""
	res3 = "struct Commit {\n  meta: struct {},\n  parents: set {\n    #4g7ggl6999v5mlucl4a507n7k3kvckiq,\n  },\n  parents_list: [\n    #4g7ggl6999v5mlucl4a507n7k3kvckiq,\n  ],\n  value: #82adk7hfcudg8fktittm672to66t6qeu,\n}"
	res4 = "[\n  \"elem1\",\n  2,\n  \"elem3\",\n]"
	res5 = "struct Commit {\n  meta: struct {},\n  parents: set {\n    #3tmg89vabs2k6hotdock1kuo13j4lmqv,\n  },\n  parents_list: [\n    #3tmg89vabs2k6hotdock1kuo13j4lmqv,\n  ],\n  value: #5cgfu2vk4nc21m1vjkjjpd2kvcm2df7q,\n}"
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
	r1, err := db.WriteValue(context.Background(), value)
	s.NoError(err)
	_, err = db.CommitValue(context.Background(), sp.GetDataset(context.Background()), r1)
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
	list, err := types.NewList(context.Background(), sp.GetDatabase(context.Background()), types.String("elem1"), types.Float(2), types.String("elem3"))
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

	// Put a value into the db, get its raw serialization, then deserialize it and ensure it comes
	// out to same thing.
	test := func(in types.Value) {
		r1, err := db.WriteValue(context.Background(), in)
		s.NoError(err)
		db.CommitValue(context.Background(), sp.GetDataset(context.Background()), r1)
		res, _ := s.MustRun(main, []string{"show", "--raw",
			spec.CreateValueSpecString("nbs", s.DBDir, "#"+r1.TargetHash().String())})
		ch := chunks.NewChunk([]byte(res))
		out, err := types.DecodeValue(ch, db)
		s.NoError(err)
		s.True(out.Equals(in))
	}

	// Primitive value with no child chunks
	test(types.String("hello"))

	// Ref (one child chunk)
	test(mustValue(db.WriteValue(context.Background(), types.Float(42))))

	// Prolly tree with multiple child chunks
	items := make([]types.Value, 10000)
	for i := 0; i < len(items); i++ {
		items[i] = types.Float(i)
	}
	l, err := types.NewList(context.Background(), db, items...)
	s.NoError(err)

	numChildChunks := 0
	_ = l.WalkRefs(db.Format(), func(r types.Ref) error {
		numChildChunks++
		return nil
	})
	s.True(numChildChunks > 0)
	test(l)
}
