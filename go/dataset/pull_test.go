// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package dataset

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func createTestDataset(name string) Dataset {
	return NewDataset(datas.NewDatabase(chunks.NewTestStore()), name)
}

func TestValidateRef(t *testing.T) {
	ds := createTestDataset("test")
	b := types.Bool(true)
	r := ds.Database().WriteValue(b)

	assert.Panics(t, func() { ds.validateRefAsCommit(r) })
	assert.Panics(t, func() { ds.validateRefAsCommit(types.NewRef(b)) })
}

func NewList(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewList(vs...)
	return ds.Database().WriteValue(v)
}

func NewMap(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewMap(vs...)
	return ds.Database().WriteValue(v)
}

func NewSet(ds Dataset, vs ...types.Value) types.Ref {
	v := types.NewSet(vs...)
	return ds.Database().WriteValue(v)
}

func TestPullTopDown(t *testing.T) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	// Give sink and source some initial shared context.
	sourceInitialValue := types.NewMap(
		types.String("first"), NewList(source),
		types.String("second"), NewList(source, types.Number(2)))
	sinkInitialValue := types.NewMap(
		types.String("first"), NewList(sink),
		types.String("second"), NewList(sink, types.Number(2)))

	var err error
	source, err = source.CommitValue(sourceInitialValue)
	assert.NoError(err)
	sink, err = sink.CommitValue(sinkInitialValue)
	assert.NoError(err)

	// Add some new stuff to source.
	updatedValue := sourceInitialValue.Set(
		types.String("third"), NewList(source, types.Number(3)))
	source, err = source.CommitValue(updatedValue)
	assert.NoError(err)

	// Add some more stuff, so that source isn't directly ahead of sink.
	updatedValue = updatedValue.Set(
		types.String("fourth"), NewList(source, types.Number(4)))
	source, err = source.CommitValue(updatedValue)
	assert.NoError(err)

	srcHeadRef := types.NewRef(source.Head())
	sink.Pull(source.Database(), srcHeadRef, 1, nil)
	sink, err = sink.FastForward(srcHeadRef)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullFirstCommitTopDown(t *testing.T) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	sourceInitialValue := types.NewMap(
		types.String("first"), NewList(source),
		types.String("second"), NewList(source, types.Number(2)))

	NewList(sink)
	NewList(sink, types.Number(2))

	source, err := source.CommitValue(sourceInitialValue)
	assert.NoError(err)

	srcHeadRef := types.NewRef(source.Head())
	sink.Pull(source.Database(), srcHeadRef, 1, nil)
	sink, err = sink.FastForward(srcHeadRef)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullDeepRefTopDown(t *testing.T) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	sourceInitialValue := types.NewList(
		types.NewList(NewList(source)),
		types.NewSet(NewSet(source)),
		types.NewMap(NewMap(source), NewMap(source)))

	source, err := source.CommitValue(sourceInitialValue)
	assert.NoError(err)

	srcHeadRef := types.NewRef(source.Head())
	sink.Pull(source.Database(), srcHeadRef, 1, nil)
	sink, err = sink.FastForward(srcHeadRef)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}

func TestPullWithMeta(t *testing.T) {
	assert := assert.New(t)

	sink := createTestDataset("sink")
	source := createTestDataset("source")

	v1 := types.Number(1)
	m1 := types.NewStruct("Meta", types.StructData{
		"name": types.String("one"),
	})
	source, err := source.Commit(v1, CommitOptions{Meta: m1})
	assert.NoError(err)

	v2 := types.Number(2)
	m2 := types.NewStruct("Meta", types.StructData{
		"name": types.String("two"),
	})
	source, err = source.Commit(v2, CommitOptions{Meta: m2})
	assert.NoError(err)
	h2 := source.Head()

	v3 := types.Number(3)
	m3 := types.NewStruct("Meta", types.StructData{
		"name": types.String("three"),
	})
	source, err = source.Commit(v3, CommitOptions{Meta: m3})
	assert.NoError(err)

	v4 := types.Number(4)
	m4 := types.NewStruct("Meta", types.StructData{
		"name": types.String("three"),
	})
	source, err = source.Commit(v4, CommitOptions{Meta: m4})
	assert.NoError(err)
	h4 := source.Head()

	srcHeadRef := types.NewRef(h2)
	sink.Pull(source.Database(), srcHeadRef, 1, nil)
	sink, err = sink.FastForward(srcHeadRef)
	assert.NoError(err)

	srcHeadRef = types.NewRef(h4)
	sink.Pull(source.Database(), srcHeadRef, 1, nil)
	sink, err = sink.FastForward(srcHeadRef)
	assert.NoError(err)
	assert.True(source.Head().Equals(sink.Head()))
}
