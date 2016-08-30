// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
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

func (s *testSuite) SetupTest() {
	exit = s.Exit
}

func (s *testSuite) TestWin() {
	sp := fmt.Sprintf("ldb:%s::test", s.LdbDir)
	ds, _ := spec.GetDataset(sp)
	ds.CommitValue(types.NewStruct("", map[string]types.Value{
		"num": types.Number(42),
		"str": types.String("foobar"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1)),
	}))
	ds.Database().Close()

	changes := map[string]string{
		".num":        "43",
		".str":        `"foobaz"`,
		".lst[0]":     "2",
		".map[1]":     `"bar"`,
		`.map["foo"]`: "2",
	}

	for k, v := range changes {
		stdout, stderr := s.Run(main, []string{sp, k, v})
		s.Equal("", stdout)
		s.Equal("", stderr)
		s.Equal(0, s.ExitStatus)
	}

	ds, _ = spec.GetDataset(sp)
	r := ds.HeadValue()
	for k, vs := range changes {
		v, _, _, _ := types.ParsePathIndex(vs)
		p, err := types.ParsePath(k)
		s.NoError(err)
		actual := p.Resolve(r)
		s.True(actual.Equals(v), "value at path %s incorrect (expected: %#v, got: %#v)", p.String(), v, actual)
	}
}

func (s *testSuite) TestLose() {
	sp := fmt.Sprintf("ldb:%s::test", s.LdbDir)
	type c struct {
		args []string
		err  string
	}
	cases := []c{
		{[]string{"foo"}, "Incorrect number of arguments\n"},
		{[]string{"foo", "bar"}, "Incorrect number of arguments\n"},
		{[]string{"foo", "bar", "baz", "quux"}, "Incorrect number of arguments\n"},
		{[]string{sp + "!!", ".foo", `"bar"`}, "Invalid input dataset '" + sp + "!!': Invalid dataset, must match [a-zA-Z0-9\\-_/]+: test!!\n"},
		{[]string{sp + "2", ".foo", `"bar"`}, "Input dataset '" + sp + "2' does not exist\n"},
		{[]string{sp, "[invalid", `"bar"`}, "Invalid path '[invalid': Invalid index: invalid\n"},
		{[]string{sp, ".nothinghere", `"bar"`}, "No value at path '.nothinghere' - cannot update\n"},
		{[]string{sp, ".foo", "bar"}, "Invalid new value: 'bar': Invalid index: bar\n"},
		{[]string{"--out-ds-name", "!invalid", sp, ".foo", `"bar"`}, "Invalid output dataset name: !invalid\n"},
		{[]string{sp, `.bar["baz"]@key`, "42"}, "Error updating path [\"baz\"]@key: @key paths not supported\n"},
		{[]string{sp, `.bar[#00000000000000000000000000000000]`, "42"}, "Invalid path '.bar[#00000000000000000000000000000000]': Invalid hash: 00000000000000000000000000000000\n"},
	}

	ds, _ := spec.GetDataset(sp)
	ds.CommitValue(types.NewStruct("", map[string]types.Value{
		"foo": types.String("foo"),
		"bar": types.NewMap(types.String("baz"), types.Number(42)),
	}))
	ds.Database().Close()

	for _, c := range cases {
		stdout, stderr := s.Run(main, c.args)
		s.Empty(stdout, "Expected empty stdout for case: %#v", c.args)
		s.Equal(c.err, stderr, "Unexpected output for case: %#v\n", c.args)
		s.Equal(1, s.ExitStatus)
	}
}
