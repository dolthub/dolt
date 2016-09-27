// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestAbsolutePathToAndFromString(t *testing.T) {
	assert := assert.New(t)

	test := func(str string) {
		p, err := NewAbsolutePath(str)
		assert.NoError(err)
		assert.Equal(str, p.String())
	}

	h := types.Number(42).Hash() // arbitrary hash
	test(fmt.Sprintf("foo.bar[#%s]", h.String()))
	test(fmt.Sprintf("#%s.bar[42]", h.String()))
}

func TestAbsolutePaths(t *testing.T) {
	assert := assert.New(t)

	s0, s1 := types.String("foo"), types.String("bar")
	list := types.NewList(s0, s1)
	emptySet := types.NewSet()

	db := datas.NewDatabase(chunks.NewMemoryStore())
	db.WriteValue(s0)
	db.WriteValue(s1)
	db.WriteValue(list)
	db.WriteValue(emptySet)

	var err error
	ds := db.GetDataset("ds")
	ds, err = db.CommitValue(ds, list)
	assert.NoError(err)
	head := ds.Head()

	resolvesTo := func(exp types.Value, str string) {
		p, err := NewAbsolutePath(str)
		assert.NoError(err)
		act := p.Resolve(db)
		if exp == nil {
			assert.Nil(act)
		} else {
			assert.True(exp.Equals(act), "%s Expected %s Actual %s", str, types.EncodedValue(exp), types.EncodedValue(act))
		}
	}

	resolvesTo(head, "ds")
	resolvesTo(emptySet, "ds.parents")
	resolvesTo(list, "ds.value")
	resolvesTo(s0, "ds.value[0]")
	resolvesTo(s1, "ds.value[1]")
	resolvesTo(head, "#"+head.Hash().String())
	resolvesTo(list, "#"+list.Hash().String())
	resolvesTo(s0, "#"+s0.Hash().String())
	resolvesTo(s1, "#"+s1.Hash().String())
	resolvesTo(s0, "#"+list.Hash().String()+"[0]")
	resolvesTo(s1, "#"+list.Hash().String()+"[1]")

	resolvesTo(nil, "foo")
	resolvesTo(nil, "foo.parents")
	resolvesTo(nil, "foo.value")
	resolvesTo(nil, "foo.value[0]")
	resolvesTo(nil, "#"+types.String("baz").Hash().String())
	resolvesTo(nil, "#"+types.String("baz").Hash().String()+"[0]")
}

func TestReadAbsolutePaths(t *testing.T) {
	assert := assert.New(t)

	s0, s1 := types.String("foo"), types.String("bar")
	list := types.NewList(s0, s1)

	db := datas.NewDatabase(chunks.NewMemoryStore())
	ds := db.GetDataset("ds")
	ds, err := db.CommitValue(ds, list)
	assert.NoError(err)

	vals, err := ReadAbsolutePaths(db, "ds.value[0]", "ds.value[1]")
	assert.NoError(err)

	assert.Equal(2, len(vals))
	assert.Equal("foo", string(vals[0].(types.String)))
	assert.Equal("bar", string(vals[1].(types.String)))

	vals, err = ReadAbsolutePaths(db, "!!#")
	assert.Nil(vals)
	assert.Equal("Invalid input path '!!#'", err.Error())

	vals, err = ReadAbsolutePaths(db, "invalid.monkey")
	assert.Nil(vals)
	assert.Equal("Input path 'invalid.monkey' does not exist in database", err.Error())
}

func TestAbsolutePathParseErrors(t *testing.T) {
	assert := assert.New(t)

	test := func(path, errMsg string) {
		p, err := NewAbsolutePath(path)
		assert.Equal(AbsolutePath{}, p)
		assert.Error(err)
		assert.Equal(errMsg, err.Error())
	}

	test("", "Empty path")
	test(".foo", "Invalid dataset name: .foo")
	test(".foo.bar.baz", "Invalid dataset name: .foo.bar.baz")
	test("#", "Invalid hash: ")
	test("#abc", "Invalid hash: abc")
	invHash := strings.Repeat("z", hash.StringLen)
	test("#"+invHash, "Invalid hash: "+invHash)
}
