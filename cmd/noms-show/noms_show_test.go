// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/samples/go/test_util"
	"github.com/attic-labs/testify/suite"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	test_util.ClientTestSuite
}

const (
	res1 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Value,\n}({\n  parents: {},\n  value: sha1-0ddf89c44a4ce4075714a4fc51d31a76e38f84ce,\n})\n"
	res2 = "\"test string\"\n"
	res3 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Value,\n}({\n  parents: {\n    sha1-46506c93810ce363c04c4bcf945797e4843a2a50,\n  },\n  value: sha1-dc6f386ab57b4ca62f54f7b63e5fe21a787f761e,\n})\n"
	res4 = "List<String | Number>([\n  \"elem1\",\n  2,\n  \"elem3\",\n])\n"
	res5 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Value,\n}({\n  parents: {\n    sha1-88ccdecaa410c101bfac396ed6a451aa14b5043b,\n  },\n  value: sha1-0ddf89c44a4ce4075714a4fc51d31a76e38f84ce,\n})\n"
)

func writeTestData(ds dataset.Dataset, value types.Value) types.Ref {
	r1 := ds.Database().WriteValue(value)
	ds, err := ds.Commit(r1)
	d.Chk.NoError(err)
	err = ds.Database().Close()
	d.Chk.NoError(err)

	return r1
}

func (s *nomsShowTestSuite) TestNomsShow() {
	datasetName := "dsTest"
	str := test_util.CreateValueSpecString("ldb", s.LdbDir, datasetName)
	sp, err := spec.ParseDatasetSpec(str)
	d.Chk.NoError(err)
	ds, err := sp.Dataset()
	d.Chk.NoError(err)

	s1 := types.String("test string")
	r := writeTestData(ds, s1)
	s.Equal(res1, s.Run(main, []string{str}))

	spec1 := test_util.CreateValueSpecString("ldb", s.LdbDir, r.TargetHash().String())
	s.Equal(res2, s.Run(main, []string{spec1}))

	ds, err = sp.Dataset()
	list := types.NewList(types.String("elem1"), types.Number(2), types.String("elem3"))
	r = writeTestData(ds, list)
	s.Equal(res3, s.Run(main, []string{str}))

	spec1 = test_util.CreateValueSpecString("ldb", s.LdbDir, r.TargetHash().String())
	s.Equal(res4, s.Run(main, []string{spec1}))

	ds, err = sp.Dataset()
	_ = writeTestData(ds, s1)
	s.Equal(res5, s.Run(main, []string{str}))
}
