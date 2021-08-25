// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/libraries/utils/osutil"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/spec"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/clienttest"
)

type nomsMergeTestSuite struct {
	clienttest.ClientTestSuite
}

func TestNomsMerge(t *testing.T) {
	suite.Run(t, &nomsMergeTestSuite{})
}

func (s *nomsMergeTestSuite) TearDownTest() {
	err := file.RemoveAll(s.DBDir)
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
			"lst": mustValue(types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"))),
			"map": mustValue(types.NewMap(context.Background(), parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"),
				types.String("foo"), types.Float(1))),
		},
		mustList(types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()))))

	l := s.setupMergeDataset(
		leftSpec,
		types.StructData{
			"num": types.Float(42),
			"str": types.String("foobaz"),
			"lst": mustValue(types.NewList(context.Background(), leftSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"))),
			"map": mustValue(types.NewMap(context.Background(), leftSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"), types.String("foo"), types.Float(1))),
		},
		mustList(types.NewList(context.Background(), leftSpec.GetDatabase(context.Background()), p)))

	r := s.setupMergeDataset(
		rightSpec,
		types.StructData{
			"num": types.Float(42),
			"str": types.String("foobar"),
			"lst": mustValue(types.NewList(context.Background(), rightSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"))),
			"map": mustValue(types.NewMap(context.Background(), rightSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"), types.String("foo"), types.Float(1), types.Float(2), types.String("bar"))),
		},
		mustList(types.NewList(context.Background(), rightSpec.GetDatabase(context.Background()), p)))

	expected := mustValue(types.NewStruct(parentSpec.GetDatabase(context.Background()).Format(), "", types.StructData{
		"num": types.Float(42),
		"str": types.String("foobaz"),
		"lst": mustValue(types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"))),
		"map": mustValue(types.NewMap(context.Background(), parentSpec.GetDatabase(context.Background()), types.Float(1), types.String("foo"), types.String("foo"), types.Float(1), types.Float(2), types.String("bar"))),
	}))

	output := "output"
	stdout, stderr, err := s.Run(main, []string{"merge", s.DBDir, left, right, output})
	if err == nil {
		s.Equal("", stderr)
		s.validateDataset(output, expected.(types.Struct), l, r)
	} else {
		s.Fail("Run failed", "err: %v\nstdout: %s\nstderr: %s\n", err, stdout, stderr)
	}
}

func (s *nomsMergeTestSuite) spec(name string) spec.Spec {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, name))
	s.NoError(err)
	return sp
}

func (s *nomsMergeTestSuite) setupMergeDataset(sp spec.Spec, data types.StructData, p types.List) types.Ref {
	ds := sp.GetDataset(context.Background())
	db := sp.GetDatabase(context.Background())
	ds, err := db.Commit(context.Background(), ds, mustValue(types.NewStruct(db.Format(), "", data)), datas.CommitOptions{ParentsList: p})
	s.NoError(err)
	return mustHeadRef(ds)
}

func (s *nomsMergeTestSuite) validateDataset(name string, expected types.Struct, parents ...types.Value) {
	sp, err := spec.ForDataset(spec.CreateValueSpecString("nbs", s.DBDir, name))
	db := sp.GetDatabase(context.Background())
	if s.NoError(err) {
		defer sp.Close()
		commit := mustHead(sp.GetDataset(context.Background()))
		s.True(mustGetValue(commit.MaybeGet(datas.ParentsField)).Equals(mustSet(types.NewSet(context.Background(), db, parents...))))
		merged := mustHeadValue(sp.GetDataset(context.Background()))
		s.True(expected.Equals(merged), "%s != %s", mustString(types.EncodedValue(context.Background(), expected)), mustString(types.EncodedValue(context.Background(), merged)))
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

	p := s.setupMergeDataset(parentSpec, types.StructData{"num": types.Float(42)}, mustList(types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()))))
	l := s.setupMergeDataset(leftSpec, types.StructData{"num": types.Float(43)}, mustList(types.NewList(context.Background(), leftSpec.GetDatabase(context.Background()), p)))
	r := s.setupMergeDataset(rightSpec, types.StructData{"num": types.Float(44)}, mustList(types.NewList(context.Background(), rightSpec.GetDatabase(context.Background()), p)))

	expected := mustValue(types.NewStruct(parentSpec.GetDatabase(context.Background()).Format(), "", types.StructData{"num": types.Float(43)}))

	output := "output"
	stdout, stderr, err := s.Run(main, []string{"merge", "--policy=l", s.DBDir, left, right, output})
	if err == nil {
		s.Equal("", stderr)
		s.validateDataset(output, expected.(types.Struct), l, r)
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

	p := s.setupMergeDataset(parentSpec, types.StructData{"num": types.Float(42)}, mustList(types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()))))
	l := s.setupMergeDataset(leftSpec, types.StructData{"num": types.Float(43)}, mustList(types.NewList(context.Background(), leftSpec.GetDatabase(context.Background()), p)))
	r := s.setupMergeDataset(rightSpec, types.StructData{"num": types.Float(44)}, mustList(types.NewList(context.Background(), rightSpec.GetDatabase(context.Background()), p)))

	expected := mustValue(types.NewStruct(parentSpec.GetDatabase(context.Background()).Format(), "", types.StructData{"num": types.Float(44)}))

	output := "output"
	stdout, stderr, err := s.Run(main, []string{"merge", "--policy=r", s.DBDir, left, right, output})
	if err == nil {
		s.Equal("", stderr)
		s.validateDataset(output, expected.(types.Struct), l, r)
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
	p := s.setupMergeDataset(parentSpec, types.StructData{"num": types.Float(42)}, mustList(types.NewList(context.Background(), parentSpec.GetDatabase(context.Background()))))
	s.setupMergeDataset(leftSpec, types.StructData{"num": types.Float(43)}, mustList(types.NewList(context.Background(), leftSpec.GetDatabase(context.Background()), p)))
	s.setupMergeDataset(rightSpec, types.StructData{"num": types.Float(44)}, mustList(types.NewList(context.Background(), rightSpec.GetDatabase(context.Background()), p)))

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
		{[]string{sp.String(), l + "!!", r, o}, "error: Invalid dataset " + l + "!!, must match [a-zA-Z0-9\\-_/]+\n"},
		{[]string{sp.String(), l + "2", r, o}, "error: Dataset " + l + "2 has no data\n"},
		{[]string{sp.String(), l, r + "2", o}, "error: Dataset " + r + "2 has no data\n"},
		{[]string{sp.String(), l, r, "!invalid"}, "error: Invalid dataset !invalid, must match [a-zA-Z0-9\\-_/]+\n"},
	}

	db := sp.GetDatabase(context.Background())

	prep := func(dsName string) {
		ds, err := db.GetDataset(context.Background(), dsName)
		s.NoError(err)
		_, err = db.CommitValue(context.Background(), ds, mustValue(types.NewMap(context.Background(), db, types.String("foo"), types.String("bar"))))
		s.NoError(err)
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
			assert.True(t, c.expected.Equals(newVal))
		}
	}
}
