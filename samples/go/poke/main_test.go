// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/marshal"
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

type testKeySub struct {
	Sub1, Sub2 string
}
type testKey struct {
	F1 testKeySub
	F2 int
}

func (s *testSuite) TestWin() {
	sp := fmt.Sprintf("ldb:%s::test", s.LdbDir)
	dsSpec, err := spec.ForDataset(sp)
	d.Chk.NoError(err)

	orig := types.NewStruct("", map[string]types.Value{
		"num": types.Number(42),
		"str": types.String("foobar"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": mustParse(map[interface{}]interface{}{
			1:        "foo",
			"foo":    1,
			"foofoo": 11,
			testKey{testKeySub{"1", "2"}, 3}: "blahblah",
		}),
	})
	db := dsSpec.GetDatabase()
	_, err = db.CommitValue(dsSpec.GetDataset(), orig)
	defer dsSpec.Close()

	tKey1 := mustParse(testKey{testKeySub{"1", "2"}, 3})
	structKeyPath1 := fmt.Sprintf(`.map[#%s]@key.f1.sub1`, tKey1.Hash())
	tKey2 := mustParse(testKey{testKeySub{"222", "2"}, 3})
	structKeyPath2 := fmt.Sprintf(`.map[#%s]@key.f1.sub1`, tKey2.Hash())

	testCases := [][]string{
		// path to set, path to check, value to check
		{".num", ".num", "43"},
		{".str", ".str", `"foobaz"`},
		{".lst[0]", ".lst[0]", "2"},
		{".map[1]", ".map[1]", `"bar"`},
		{`.map["foo"]`, `.map["foo"]`, "2"},
		{`.map["foofoo"]@key`, `.map["barbar"]@key`, `"barbar"`},
		{structKeyPath1, structKeyPath2, `"222"`},
	}

	for _, tc := range testCases {
		pathToSet, nv := tc[0], tc[2]
		stdout, stderr, err := s.Run(main, []string{sp, pathToSet, nv})
		s.Equal("", stdout)
		s.Equal("", stderr)
		s.Equal(nil, err)
	}

	dsSpec, err = spec.ForDataset(sp)
	d.Chk.NoError(err)
	defer dsSpec.Close()

	r := dsSpec.GetDataset().HeadValue()
	for _, tc := range testCases {
		pathToCheck, vs := tc[1], tc[2]
		v, _, _, _ := types.ParsePathIndex(vs)
		p, err := types.ParsePath(pathToCheck)
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

func mustParse(v interface{}) types.Value {
	nv, err := marshal.Marshal(v)
	d.Chk.NoError(err)
	return nv
}
