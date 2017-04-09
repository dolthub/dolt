// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/noms/go/util/test"
	"github.com/attic-labs/testify/suite"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	clienttest.ClientTestSuite
}

const (
	res1 = "Commit {\n  meta:  {},\n  parents: {},\n  value: nl181uu1ioc2j6t7mt9paidjlhlcjtgj,\n}\n"
	res2 = "\"test string\"\n"
	res3 = "Commit {\n  meta:  {},\n  parents: {\n    4g7ggl6999v5mlucl4a507n7k3kvckiq,\n  },\n  value: 82adk7hfcudg8fktittm672to66t6qeu,\n}\n"
	res4 = "[\n  \"elem1\",\n  2,\n  \"elem3\",\n]\n"
	res5 = "Commit {\n  meta:  {},\n  parents: {\n    3tmg89vabs2k6hotdock1kuo13j4lmqv,\n  },\n  value: 5cgfu2vk4nc21m1vjkjjpd2kvcm2df7q,\n}\n"
)

func (s *nomsShowTestSuite) writeTestData(str string, value types.Value) types.Ref {
	sp, err := spec.ForDataset(str)
	s.NoError(err)
	defer sp.Close()

	db := sp.GetDatabase()
	r1 := db.WriteValue(value)
	_, err = db.CommitValue(sp.GetDataset(), r1)
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

	list := types.NewList(types.String("elem1"), types.Number(2), types.String("elem3"))
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

	db := sp.GetDatabase()

	// Put a value into the db, get its raw serialization, then deserialize it and ensure it comes
	// out to same thing.
	test := func(in types.Value) {
		r1 := db.WriteValue(in)
		db.CommitValue(sp.GetDataset(), r1)
		res, _ := s.MustRun(main, []string{"show", "--raw",
			spec.CreateValueSpecString("nbs", s.DBDir, "#"+r1.TargetHash().String())})
		ch := chunks.NewChunk([]byte(res))
		out := types.DecodeValue(ch, db)
		s.True(out.Equals(in))
	}

	// Primitive value with no child chunks
	test(types.String("hello"))

	// Ref (one child chunk)
	test(db.WriteValue(types.Number(42)))

	// Prolly tree with multiple child chunks
	items := make([]types.Value, 10000)
	for i := 0; i < len(items); i++ {
		items[i] = types.Number(i)
	}
	l := types.NewList(items...)
	numChildChunks := 0
	l.WalkRefs(func(r types.Ref) {
		numChildChunks++
	})
	s.True(numChildChunks > 0)
	test(l)
}
