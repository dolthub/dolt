// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	clienttest.ClientTestSuite
}

const (
	res1 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Value,\n}({\n  parents: {},\n  value: sha1-0ddf89c44a4ce4075714a4fc51d31a76e38f84ce,\n})\n"
	res2 = "\"test string\"\n"
	res3 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Value,\n}({\n  parents: {\n    sha1-46506c93810ce363c04c4bcf945797e4843a2a50,\n  },\n  value: sha1-46f553ffca109477301aca47b2576ac5d2a8ec9b,\n})\n"
	res4 = "List<String | Number>([\n  \"elem1\",\n  2,\n  \"elem3\",\n])\n"
	res5 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Value,\n}({\n  parents: {\n    sha1-024d53e8f0adc94f1a8a81d265255fb2a5c8233b,\n  },\n  value: sha1-0ddf89c44a4ce4075714a4fc51d31a76e38f84ce,\n})\n"
)

func writeTestData(str string, value types.Value) types.Ref {
	ds, err := spec.GetDataset(str)
	d.Chk.NoError(err)

	r1 := ds.Database().WriteValue(value)
	ds, err = ds.Commit(r1)
	d.Chk.NoError(err)

	err = ds.Database().Close()
	d.Chk.NoError(err)
	return r1
}

func (s *nomsShowTestSuite) TestNomsShow() {
	datasetName := "dsTest"
	str := spec.CreateValueSpecString("ldb", s.LdbDir, datasetName)

	s1 := types.String("test string")
	r := writeTestData(str, s1)
	res, _ := s.Run(main, []string{"show", str})
	s.Equal(res1, res)

	str1 := spec.CreateValueSpecString("ldb", s.LdbDir, "#"+r.TargetHash().String())
	res, _ = s.Run(main, []string{"show", str1})
	s.Equal(res2, res)

	list := types.NewList(types.String("elem1"), types.Number(2), types.String("elem3"))
	r = writeTestData(str, list)
	res, _ = s.Run(main, []string{"show", str})
	s.Equal(res3, res)

	str1 = spec.CreateValueSpecString("ldb", s.LdbDir, "#"+r.TargetHash().String())
	res, _ = s.Run(main, []string{"show", str1})
	s.Equal(res4, res)

	_ = writeTestData(str, s1)
	res, _ = s.Run(main, []string{"show", str})
	s.Equal(res5, res)
}
