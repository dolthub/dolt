// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/osutil"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/clienttest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type nomsMergeTestSuite struct {
	clienttest.ClientTestSuite
}

func TestNomsMerge(t *testing.T) {
	suite.Run(t, &nomsMergeTestSuite{})
}

func (s *nomsMergeTestSuite) TearDownTest() {
	err := os.RemoveAll(s.DBDir)
	if !osutil.IsWindows {
		s.NoError(err)
	}
}

func (s *nomsMergeTestSuite) TestNomsMerge_Success() {
	left, right := "left", "right"
	parentSpec := s.spec("parent")
	defer parentSpec.Close()
	leftSpec := s.spec(left)
	defer leftSpec.Close()
	rightSpec := s.spec(right)
	defer rightSpec.Close()

	p := s.setupMergeDataset(
		parentSpec,
		types.StructData{
			"num": types.Float(42),
			"str": types.String("foobar"),
			"lst": types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo")),
			"map": types.NewMap(context.Background(), types.Format_7_18, parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"),
				types.String("foo"), types.Float(1)),
		},
		types.NewSet(context.Background(), types.Format_7_18, parentSpec.GetDatabase(context.Background())))

	l := s.setupMergeDataset(
		leftSpec,
		types.StructData{
			"num": types.Float(42),
			"str": types.String("foobaz"),
			"lst": types.NewList(context.Background(), leftSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo")),
			"map": types.NewMap(context.Background(), types.Format_7_18, leftSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"),
				types.String("foo"), types.Float(1)),
		},
		types.NewSet(context.Background(), types.Format_7_18, leftSpec.GetDatabase(context.Background()), p))

	r := s.setupMergeDataset(
		rightSpec,
		types.StructData{
			"num": types.Float(42),
			"str": types.String("foobar"),
			"lst": types.NewList(context.Background(), rightSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo")),
			"map": types.NewMap(context.Background(), types.Format_7_18, rightSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"),
				types.String("foo"), types.Float(1), types.Float(2), types.String("bar")),
		},
		types.NewSet(context.Background(), types.Format_7_18, rightSpec.GetDatabase(context.Background()), p))

	expected := types.NewStruct(types.Format_7_18, "", types.StructData{
		"num": types.Float(42),
		"str": types.String("foobaz"),
		"lst": types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo")),
		"map": types.NewMap(context.Background(), types.Format_7_18, parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"),
			types.String("foo"), types.Float(1), types.Float(2), types.String("bar")),
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

func (s *nomsMergeTestSuite) spec(name string) spec.Spec {
	sp, err := spec.ForDataset(types.Format_7_18, spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, name))
	s.NoError(err)
	return sp
}

func (s *nomsMergeTestSuite) setupMergeDataset(sp spec.Spec, data types.StructData, p types.Set) types.Ref {
	ds := sp.GetDataset(context.Background())
	ds, err := sp.GetDatabase(context.Background()).Commit(context.Background(), ds, types.NewStruct(types.Format_7_18, "", data), datas.CommitOptions{Parents: p})
	s.NoError(err)
	return ds.HeadRef()
}

func (s *nomsMergeTestSuite) validateDataset(name string, expected types.Struct, parents ...types.Value) {
	sp, err := spec.ForDataset(types.Format_7_18, spec.CreateValueSpecString(types.Format_7_18, "nbs", s.DBDir, name))
	db := sp.GetDatabase(context.Background())
	if s.NoError(err) {
		defer sp.Close()
		commit := sp.GetDataset(context.Background()).Head()
		s.True(commit.Get(datas.ParentsField).Equals(types.Format_7_18, types.NewSet(context.Background(), types.Format_7_18, db, parents...)))
		merged := sp.GetDataset(context.Background()).HeadValue()
		s.True(expected.Equals(types.Format_7_18, merged), "%s != %s", types.EncodedValue(context.Background(), types.Format_7_18, expected), types.EncodedValue(context.Background(), types.Format_7_18, merged))
	}
}

func (s *nomsMergeTestSuite) TestNomsMerge_Left() {
	left, right := "left", "right"
	parentSpec := s.spec("parent")
	defer parentSpec.Close()
	leftSpec := s.spec(left)
	defer leftSpec.Close()
	rightSpec := s.spec(right)
	defer rightSpec.Close()

	p := s.setupMergeDataset(parentSpec, types.StructData{"num": types.Float(42)}, types.NewSet(context.Background(), types.Format_7_18, parentSpec.GetDatabase(context.Background())))
	l := s.setupMergeDataset(leftSpec, types.StructData{"num": types.Float(43)}, types.NewSet(context.Background(), types.Format_7_18, leftSpec.GetDatabase(context.Background()), p))
	r := s.setupMergeDataset(rightSpec, types.StructData{"num": types.Float(44)}, types.NewSet(context.Background(), types.Format_7_18, rightSpec.GetDatabase(context.Background()), p))

	expected := types.NewStruct(types.Format_7_18, "", types.StructData{"num": types.Float(43)})

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
	parentSpec := s.spec("parent")
	defer parentSpec.Close()
	leftSpec := s.spec(left)
	defer leftSpec.Close()
	rightSpec := s.spec(right)
	defer rightSpec.Close()

	p := s.setupMergeDataset(parentSpec, types.StructData{"num": types.Float(42)}, types.NewSet(context.Background(), types.Format_7_18, parentSpec.GetDatabase(context.Background())))
	l := s.setupMergeDataset(leftSpec, types.StructData{"num": types.Float(43)}, types.NewSet(context.Background(), types.Format_7_18, leftSpec.GetDatabase(context.Background()), p))
	r := s.setupMergeDataset(rightSpec, types.StructData{"num": types.Float(44)}, types.NewSet(context.Background(), types.Format_7_18, rightSpec.GetDatabase(context.Background()), p))

	expected := types.NewStruct(types.Format_7_18, "", types.StructData{"num": types.Float(44)})

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
	parentSpec := s.spec("parent")
	defer parentSpec.Close()
	leftSpec := s.spec(left)
	defer leftSpec.Close()
	rightSpec := s.spec(right)
	defer rightSpec.Close()
	p := s.setupMergeDataset(parentSpec, types.StructData{"num": types.Float(42)}, types.NewSet(context.Background(), types.Format_7_18, parentSpec.GetDatabase(context.Background())))
	s.setupMergeDataset(leftSpec, types.StructData{"num": types.Float(43)}, types.NewSet(context.Background(), types.Format_7_18, leftSpec.GetDatabase(context.Background()), p))
	s.setupMergeDataset(rightSpec, types.StructData{"num": types.Float(44)}, types.NewSet(context.Background(), types.Format_7_18, rightSpec.GetDatabase(context.Background()), p))

	s.Panics(func() { s.MustRun(main, []string{"merge", s.DBDir, left, right, "output"}) })
}

func (s *nomsMergeTestSuite) TestBadInput() {
	sp, err := spec.ForDatabase(spec.CreateDatabaseSpecString(types.Format_7_18, "nbs", s.DBDir))
	s.NoError(err)
	defer sp.Close()

	l, r, o := "left", "right", "output"
	type c struct {
		args []string
		err  string
	}
	cases := []c{
		{[]string{sp.String(types.Format_7_18), l + "!!", r, o}, "error: Invalid dataset " + l + "!!, must match [a-zA-Z0-9\\-_/]+\n"},
		{[]string{sp.String(types.Format_7_18), l + "2", r, o}, "error: Dataset " + l + "2 has no data\n"},
		{[]string{sp.String(types.Format_7_18), l, r + "2", o}, "error: Dataset " + r + "2 has no data\n"},
		{[]string{sp.String(types.Format_7_18), l, r, "!invalid"}, "error: Invalid dataset !invalid, must match [a-zA-Z0-9\\-_/]+\n"},
	}

	db := sp.GetDatabase(context.Background())

	prep := func(dsName string) {
		ds := db.GetDataset(context.Background(), dsName)
		db.CommitValue(context.Background(), ds, types.NewMap(context.Background(), types.Format_7_18, db, types.String("foo"), types.String("bar")))
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
		{"l\n", types.DiffChangeAdded, types.DiffChangeAdded, types.Float(7), types.String("bar"), types.DiffChangeAdded, types.Float(7), true},
		{"r\n", types.DiffChangeModified, types.DiffChangeModified, types.Float(7), types.String("bar"), types.DiffChangeModified, types.String("bar"), true},
	}

	for _, c := range cases {
		input := bytes.NewBufferString(c.input)

		changeType, newVal, ok := cliResolve(input, ioutil.Discard, c.aChange, c.bChange, c.aVal, c.bVal, types.Path{})
		if !c.success {
			assert.False(t, ok)
		} else if assert.True(t, ok) {
			assert.Equal(t, c.expectedChange, changeType)
			assert.True(t, c.expected.Equals(types.Format_7_18, newVal))
		}
	}
}
