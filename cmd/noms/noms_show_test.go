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
	res1 = "struct Commit {\n  parents: Set<Ref<Cycle<0>>>,\n  value: Ref<String>,\n}({\n  parents: {},\n  value: 5cgfu2vk4nc21m1vjkjjpd2kvcm2df7q,\n})\n"
	res2 = "\"test string\"\n"
	res3 = "struct Commit {\n  parents: Set<Ref<struct Commit {\n    parents: Set<Ref<Cycle<0>>>,\n    value: Ref<String> | Ref<List<Number | String>>,\n  }>>,\n  value: Ref<List<Number | String>>,\n}({\n  parents: {\n    tenjp25hbcqm6m73pm3k5hrpihse9tk8,\n  },\n  value: 82adk7hfcudg8fktittm672to66t6qeu,\n})\n"
	res4 = "List<Number | String>([\n  \"elem1\",\n  2,\n  \"elem3\",\n])\n"
	res5 = "struct Commit {\n  parents: Set<Ref<struct Commit {\n    parents: Set<Ref<Cycle<0>>>,\n    value: Ref<String> | Ref<List<Number | String>>,\n  }>>,\n  value: Ref<String>,\n}({\n  parents: {\n    5vfutgr7bjj9lk982b2mj1sirj9qenqv,\n  },\n  value: 5cgfu2vk4nc21m1vjkjjpd2kvcm2df7q,\n})\n"
)

func writeTestData(str string, value types.Value) types.Ref {
	ds, err := spec.GetDataset(str)
	d.Chk.NoError(err)

	r1 := ds.Database().WriteValue(value)
	ds, err = ds.CommitValue(r1)
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
