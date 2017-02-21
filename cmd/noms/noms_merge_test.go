// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/clienttest"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

type nomsMergeTestSuite struct {
	clienttest.ClientTestSuite
}

func TestNomsMerge(t *testing.T) {
	suite.Run(t, &nomsMergeTestSuite{})
}

func (s *nomsMergeTestSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.DBDir))
}

func (s *nomsMergeTestSuite) TestNomsMerge_Success() {
	left, right := "left", "right"
	p := s.setupMergeDataset(
		"parent",
		types.StructData{
			"num": types.Number(42),
			"str": types.String("foobar"),
			"lst": types.NewList(types.Number(1), types.String("foo")),
			"map": types.NewMap(types.Number(1), types.String("foo"),
				types.String("foo"), types.Number(1)),
		},
		types.NewSet())

	l := s.setupMergeDataset(
		left,
		types.StructData{
			"num": types.Number(42),
			"str": types.String("foobaz"),
			"lst": types.NewList(types.Number(1), types.String("foo")),
			"map": types.NewMap(types.Number(1), types.String("foo"),
				types.String("foo"), types.Number(1)),
		},
		types.NewSet(p))

	r := s.setupMergeDataset(
		right,
		types.StructData{
			"num": types.Number(42),
			"str": types.String("foobar"),
			"lst": types.NewList(types.Number(1), types.String("foo")),
			"map": types.NewMap(types.Number(1), types.String("foo"),
				types.String("foo"), types.Number(1), types.Number(2), types.String("bar")),
		},
		types.NewSet(p))

	expected := types.NewStruct("", types.StructData{
		"num": types.Number(42),
		"str": types.String("foobaz"),
		"lst": types.NewList(types.Number(1), types.String("foo")),
		"map": types.NewMap(types.Number(1), types.String("foo"),
			types.String("foo"), types.Number(1), types.Number(2), types.String("bar")),
	})

	output := "output"
	stdout, stderr, err := s.Run(main, []string{"merge", s.DBDir, left, right, output})
	if err == nil {
		s.Equal("", stderr)
		s.validateDataset(output, expected, l, r)
	} else {
		s.Fail("Run failed", "err: %v\nstdout: %s\nstderr: %s\n", err, stdout, stderr)
	}
}

func (s *nomsMergeTestSuite) setupMergeDataset(name string, data types.StructData, p types.Set) types.Ref {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, name))
	s.NoError(err)
	defer sp.Close()

	ds := sp.GetDataset()
	ds, err = sp.GetDatabase().Commit(ds, types.NewStruct("", data), datas.CommitOptions{Parents: p})
	s.NoError(err)
	return ds.HeadRef()
}

func (s *nomsMergeTestSuite) validateDataset(name string, expected types.Struct, parents ...types.Value) {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, name))
	if s.NoError(err) {
		defer sp.Close()
		commit := sp.GetDataset().Head()
		s.True(commit.Get(datas.ParentsField).Equals(types.NewSet(parents...)))
		merged := sp.GetDataset().HeadValue()
		s.True(expected.Equals(merged), "%s != %s", types.EncodedValue(expected), types.EncodedValue(merged))
	}
}

func (s *nomsMergeTestSuite) TestNomsMerge_Left() {
	left, right := "left", "right"
	p := s.setupMergeDataset("parent", types.StructData{"num": types.Number(42)}, types.NewSet())
	l := s.setupMergeDataset(left, types.StructData{"num": types.Number(43)}, types.NewSet(p))
	r := s.setupMergeDataset(right, types.StructData{"num": types.Number(44)}, types.NewSet(p))

	expected := types.NewStruct("", types.StructData{"num": types.Number(43)})

	output := "output"
	stdout, stderr, err := s.Run(main, []string{"merge", "--policy=l", s.DBDir, left, right, output})
	if err == nil {
		s.Equal("", stderr)
		s.validateDataset(output, expected, l, r)
	} else {
		s.Fail("Run failed", "err: %v\nstdout: %s\nstderr: %s\n", err, stdout, stderr)
	}
}

func (s *nomsMergeTestSuite) TestNomsMerge_Right() {
	left, right := "left", "right"
	p := s.setupMergeDataset("parent", types.StructData{"num": types.Number(42)}, types.NewSet())
	l := s.setupMergeDataset(left, types.StructData{"num": types.Number(43)}, types.NewSet(p))
	r := s.setupMergeDataset(right, types.StructData{"num": types.Number(44)}, types.NewSet(p))

	expected := types.NewStruct("", types.StructData{"num": types.Number(44)})

	output := "output"
	stdout, stderr, err := s.Run(main, []string{"merge", "--policy=r", s.DBDir, left, right, output})
	if err == nil {
		s.Equal("", stderr)
		s.validateDataset(output, expected, l, r)
	} else {
		s.Fail("Run failed", "err: %v\nstdout: %s\nstderr: %s\n", err, stdout, stderr)
	}
}

func (s *nomsMergeTestSuite) TestNomsMerge_Conflict() {
	left, right := "left", "right"
	p := s.setupMergeDataset("parent", types.StructData{"num": types.Number(42)}, types.NewSet())
	s.setupMergeDataset(left, types.StructData{"num": types.Number(43)}, types.NewSet(p))
	s.setupMergeDataset(right, types.StructData{"num": types.Number(44)}, types.NewSet(p))

	s.Panics(func() { s.MustRun(main, []string{"merge", s.DBDir, left, right, "output"}) })
}

func (s *nomsMergeTestSuite) TestBadInput() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString("nbs", s.DBDir))
	s.NoError(err)
	defer sp.Close()

	l, r, o := "left", "right", "output"
	type c struct {
		args []string
		err  string
	}
	cases := []c{
		{[]string{"foo"}, "error: Incorrect number of arguments\n"},
		{[]string{"foo", "bar"}, "error: Incorrect number of arguments\n"},
		{[]string{"foo", "bar", "baz"}, "error: Incorrect number of arguments\n"},
		{[]string{"foo", "bar", "baz", "quux", "five"}, "error: Incorrect number of arguments\n"},
		{[]string{sp.String(), l + "!!", r, o}, "error: Invalid dataset " + l + "!!, must match [a-zA-Z0-9\\-_/]+\n"},
		{[]string{sp.String(), l + "2", r, o}, "error: Dataset " + l + "2 has no data\n"},
		{[]string{sp.String(), l, r + "2", o}, "error: Dataset " + r + "2 has no data\n"},
		{[]string{sp.String(), l, r, "!invalid"}, "error: Invalid dataset !invalid, must match [a-zA-Z0-9\\-_/]+\n"},
	}

	db := sp.GetDatabase()

	prep := func(dsName string) {
		ds := db.GetDataset(dsName)
		db.CommitValue(ds, types.NewMap(types.String("foo"), types.String("bar")))
	}
	prep(l)
	prep(r)

	for _, c := range cases {
		stdout, stderr, err := s.Run(main, append([]string{"merge"}, c.args...))
		s.Empty(stdout, "Expected empty stdout for case: %#v", c.args)
		if !s.NotNil(err, "Unexpected success for case: %#v\n", c.args) {
			continue
		}
		if mainErr, ok := err.(clienttest.ExitError); ok {
			s.Equal(1, mainErr.Code)
			s.Equal(c.err, stderr, "Unexpected output for case: %#v\n", c.args)
		} else {
			s.Fail("Run() recovered non-error panic", "err: %#v\nstdout: %s\nstderr: %s\n", err, stdout, stderr)
		}
	}
}

func TestNomsMergeCliResolve(t *testing.T) {
	type c struct {
		input            string
		aChange, bChange types.DiffChangeType
		aVal, bVal       types.Value
		expectedChange   types.DiffChangeType
		expected         types.Value
		success          bool
	}

	cases := []c{
		{"l\n", types.DiffChangeAdded, types.DiffChangeAdded, types.String("foo"), types.String("bar"), types.DiffChangeAdded, types.String("foo"), true},
		{"r\n", types.DiffChangeAdded, types.DiffChangeAdded, types.String("foo"), types.String("bar"), types.DiffChangeAdded, types.String("bar"), true},
		{"l\n", types.DiffChangeAdded, types.DiffChangeAdded, types.Number(7), types.String("bar"), types.DiffChangeAdded, types.Number(7), true},
		{"r\n", types.DiffChangeModified, types.DiffChangeModified, types.Number(7), types.String("bar"), types.DiffChangeModified, types.String("bar"), true},
	}

	for _, c := range cases {
		input := bytes.NewBufferString(c.input)

		changeType, newVal, ok := cliResolve(input, ioutil.Discard, c.aChange, c.bChange, c.aVal, c.bVal, types.Path{})
		if !c.success {
			assert.False(t, ok)
		} else if assert.True(t, ok) {
			assert.Equal(t, c.expectedChange, changeType)
			assert.True(t, c.expected.Equals(newVal))
		}
	}
}
