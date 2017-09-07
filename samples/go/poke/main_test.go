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
	"github.com/stretchr/testify/suite"
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
	sp := spec.CreateValueSpecString("nbs", s.DBDir, "test")
	dsSpec, err := spec.ForDataset(sp)
	d.Chk.NoError(err)
	db := dsSpec.GetDatabase()

	orig := types.NewStruct("", map[string]types.Value{
		"num": types.Number(42),
		"str": types.String("foobar"),
		"lst": types.NewList(db, types.Number(1), types.String("foo")),
		"map": mustParse(db, map[interface{}]interface{}{
			1:        "foo",
			"foo":    1,
			"foofoo": 11,
			testKey{testKeySub{"1", "2"}, 3}: "blahblah",
		}),
	})
	_, err = db.CommitValue(dsSpec.GetDataset(), orig)
	defer dsSpec.Close()

	tKey1 := mustParse(db, testKey{testKeySub{"1", "2"}, 3})
	structKeyPath1 := fmt.Sprintf(`.map[#%s]@key.f1.sub1`, tKey1.Hash())
	tKey2 := mustParse(db, testKey{testKeySub{"222", "2"}, 3})
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
		actual := p.Resolve(r, dsSpec.GetDatabase())
		s.True(actual.Equals(v), "value at path %s incorrect (expected: %#v, got: %#v)", p.String(), v, actual)
	}
}

func (s *testSuite) TestLose() {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, "test"))
	s.NoError(err)
	defer sp.Close()

	cases := []struct {
		args []string
		err  string
	}{
		{[]string{"foo"}, "Incorrect number of arguments\n"},
		{[]string{"foo", "bar"}, "Incorrect number of arguments\n"},
		{[]string{"foo", "bar", "baz", "quux"}, "Incorrect number of arguments\n"},
		{[]string{sp.String() + "!!", ".foo", `"bar"`}, "Invalid input dataset '" + sp.String() + "!!': Invalid operator: !\n"},
		{[]string{sp.String() + "2", ".foo", `"bar"`}, "Input dataset '" + sp.String() + "2' does not exist\n"},
		{[]string{sp.String(), "[invalid", `"bar"`}, "Invalid path '[invalid': Invalid index: invalid\n"},
		{[]string{sp.String(), ".nothinghere", `"bar"`}, "No value at path '.nothinghere' - cannot update\n"},
		{[]string{sp.String(), ".foo", "bar"}, "Invalid new value: 'bar': Invalid index: bar\n"},
		{[]string{"--out-ds-name", "!invalid", sp.String(), ".foo", `"bar"`}, "Invalid output dataset name: !invalid\n"},
		{[]string{sp.String(), `.bar[#00000000000000000000000000000000]`, "42"}, "Invalid path '.bar[#00000000000000000000000000000000]': Invalid hash: 00000000000000000000000000000000\n"},
	}

	sp.GetDatabase().CommitValue(sp.GetDataset(), types.NewStruct("", map[string]types.Value{
		"foo": types.String("foo"),
		"bar": types.NewMap(sp.GetDatabase(), types.String("baz"), types.Number(42)),
	}))

	for _, c := range cases {
		stdout, stderr, err := s.Run(main, c.args)
		s.Empty(stdout, "Expected empty stdout for case: %#v", c.args)
		s.Equal(c.err, stderr, "Unexpected output for case: %#v\n", c.args)
		s.Equal(clienttest.ExitError{1}, err)
	}
}

func mustParse(vrw types.ValueReadWriter, v interface{}) types.Value {
	nv, err := marshal.Marshal(vrw, v)
	d.Chk.NoError(err)
	return nv
}
