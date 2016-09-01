// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"testing"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/suite"
)

func TestBasics(t *testing.T) {
	suite.Run(t, &testSuite{})
}

type testSuite struct {
	clienttest.ClientTestSuite
}

func (s *testSuite) TestWin() {
	prep := func(name string, data types.StructData) {
		ds, _ := spec.GetDataset(spec.CreateValueSpecString("ldb", s.LdbDir, name))
		defer ds.Database().Close()
		ds.CommitValue(types.NewStruct("", data))
	}

	p := "parent"
	prep(p, types.StructData{
		"num": types.Number(42),
		"str": types.String("foobar"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1)),
	})

	l := "left"
	prep(l, types.StructData{
		"num": types.Number(42),
		"str": types.String("foobaz"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1)),
	})

	r := "right"
	prep(r, types.StructData{
		"num": types.Number(42),
		"str": types.String("foobar"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1), types.Number(2), types.String("bar")),
	})

	expected := types.NewStruct("", types.StructData{
		"num": types.Number(42),
		"str": types.String("foobaz"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1), types.Number(2), types.String("bar")),
	})

	var mainErr error
	stdout, stderr := s.Run(func() { mainErr = nomsMerge() }, []string{"--quiet=true", "--parent=" + p, s.LdbDir, l, r})
	if s.NoError(mainErr, "%s", mainErr) {
		s.Equal("", stdout)
		s.Equal("", stderr)

		ds, err := spec.GetDataset(spec.CreateValueSpecString("ldb", s.LdbDir, r))
		if s.NoError(err) {
			merged := ds.HeadValue()
			s.True(expected.Equals(merged), "%s != %s", types.EncodedValue(expected), types.EncodedValue(merged))
		}
		defer ds.Database().Close()
	}
}

// TODO failure tests
