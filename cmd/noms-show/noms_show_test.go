// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/test_util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/testify/suite"
)

func TestNomsShow(t *testing.T) {
	suite.Run(t, &nomsShowTestSuite{})
}

type nomsShowTestSuite struct {
	test_util.ClientTestSuite
}

const (
	res1 = "struct Commit {\n  parents: Set<Ref<Parent<0>>>\n  value: Value\n}({\n  parents: {},\n  value: sha1-7cecea5a8e3bcf30b35984eb7f592d98fa994a9a,\n})\n"
	res2 = "\"test string\"\n"
	res3 = "struct Commit {\n  parents: Set<Ref<Parent<0>>>\n  value: Value\n}({\n  parents: {\n    sha1-f9c7cd9a706d9e6e9b2af0f2c39a971490273ff9,\n  },\n  value: sha1-fa99fbfa191aba579133bf0bde029ff8c08e6f62,\n})\n"
	res4 = "List<Number | String>([\n  \"elem1\",\n  2,\n  \"elem3\",\n])\n"
	res5 = "struct Commit {\n  parents: Set<Ref<Parent<0>>>\n  value: Value\n}({\n  parents: {\n    sha1-bad0c2fc94b2666ab774792911c57e8ec8ba1285,\n  },\n  value: sha1-7cecea5a8e3bcf30b35984eb7f592d98fa994a9a,\n})\n"
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
	spec := fmt.Sprintf("ldb:%s:%s", s.LdbDir, datasetName)
	sp, err := flags.ParseDatasetSpec(spec)
	d.Chk.NoError(err)
	ds, err := sp.Dataset()
	d.Chk.NoError(err)

	s1 := types.NewString("test string")
	r := writeTestData(ds, s1)
	s.Equal(res1, s.Run(main, []string{spec}))

	spec1 := fmt.Sprintf("ldb:%s:%s", s.LdbDir, r.TargetHash().String())
	s.Equal(res2, s.Run(main, []string{spec1}))

	ds, err = sp.Dataset()
	list := types.NewList(types.NewString("elem1"), types.Number(2), types.NewString("elem3"))
	r = writeTestData(ds, list)
	s.Equal(res3, s.Run(main, []string{spec}))

	spec1 = fmt.Sprintf("ldb:%s:%s", s.LdbDir, r.TargetHash().String())
	s.Equal(res4, s.Run(main, []string{spec1}))

	ds, err = sp.Dataset()
	_ = writeTestData(ds, s1)
	s.Equal(res5, s.Run(main, []string{spec}))
}
