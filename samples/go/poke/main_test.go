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

func (s *testSuite) TestWin() {
	sp, err := spec.ForDataset(fmt.Sprintf("ldb:%s::test", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	sp.GetDatabase().CommitValue(sp.GetDataset(), types.NewStruct("", map[string]types.Value{
		"num": types.Number(42),
		"str": types.String("foobar"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1)),
	}))

	changes := map[string]string{
		".num":        "43",
		".str":        `"foobaz"`,
		".lst[0]":     "2",
		".map[1]":     `"bar"`,
		`.map["foo"]`: "2",
	}

	for k, v := range changes {
		stdout, stderr, err := s.Run(main, []string{sp.Spec, k, v})
		s.Equal("", stdout)
		s.Equal("", stderr)
		s.Equal(nil, err)
	}

	sp, _ = spec.ForDataset(sp.Spec)
	defer sp.Close()

	r := sp.GetDataset().HeadValue()
	for k, vs := range changes {
		v, _, _, _ := types.ParsePathIndex(vs)
		p, err := types.ParsePath(k)
		s.NoError(err)
		actual := p.Resolve(r)
		s.True(actual.Equals(v), "value at path %s incorrect (expected: %#v, got: %#v)", p.String(), v, actual)
	}
}

func (s *testSuite) TestLose() {
	sp, err := spec.ForDataset(fmt.Sprintf("ldb:%s::test", s.LdbDir))
	s.NoError(err)
	defer sp.Close()

	cases := []struct {
		args []string
		err  string
	}{
		{[]string{"foo"}, "Incorrect number of arguments\n"},
		{[]string{"foo", "bar"}, "Incorrect number of arguments\n"},
		{[]string{"foo", "bar", "baz", "quux"}, "Incorrect number of arguments\n"},
		{[]string{sp.Spec + "!!", ".foo", `"bar"`}, "Invalid input dataset '" + sp.Spec + "!!': Dataset test!! must match ^[a-zA-Z0-9\\-_/]+$\n"},
		{[]string{sp.Spec + "2", ".foo", `"bar"`}, "Input dataset '" + sp.Spec + "2' does not exist\n"},
		{[]string{sp.Spec, "[invalid", `"bar"`}, "Invalid path '[invalid': Invalid index: invalid\n"},
		{[]string{sp.Spec, ".nothinghere", `"bar"`}, "No value at path '.nothinghere' - cannot update\n"},
		{[]string{sp.Spec, ".foo", "bar"}, "Invalid new value: 'bar': Invalid index: bar\n"},
		{[]string{"--out-ds-name", "!invalid", sp.Spec, ".foo", `"bar"`}, "Invalid output dataset name: !invalid\n"},
		{[]string{sp.Spec, `.bar["baz"]@key`, "42"}, "Error updating path [\"baz\"]@key: @key paths not supported\n"},
		{[]string{sp.Spec, `.bar[#00000000000000000000000000000000]`, "42"}, "Invalid path '.bar[#00000000000000000000000000000000]': Invalid hash: 00000000000000000000000000000000\n"},
	}

	sp.GetDatabase().CommitValue(sp.GetDataset(), types.NewStruct("", map[string]types.Value{
		"foo": types.String("foo"),
		"bar": types.NewMap(types.String("baz"), types.Number(42)),
	}))

	for _, c := range cases {
		stdout, stderr, err := s.Run(main, c.args)
		s.Empty(stdout, "Expected empty stdout for case: %#v", c.args)
		s.Equal(c.err, stderr, "Unexpected output for case: %#v\n", c.args)
		s.Equal(clienttest.ExitError{1}, err)
	}
}
