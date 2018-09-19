// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	a := assert.New(t)

	storage := &chunks.MemoryStorage{}
	vs := types.NewValueStore(storage.NewView())
	defer vs.Close()

	docs := []struct {
		terms string
		id    int
	}{
		{"foo bar baz", 1},
		{"foo baz", 2},
		{"baz bat boo", 3},
	}

	indexEditor := NewTermIndex(vs, types.NewMap(vs)).Edit()
	for _, doc := range docs {
		indexEditor.InsertAll(strings.Split(doc.terms, " "), types.Float(doc.id))
	}

	index := indexEditor.Value()

	getMap := func(keys ...int) types.Map {
		m := types.NewMap(vs).Edit()
		for _, k := range keys {
			m.Set(types.Float(k), types.Bool(true))
		}
		return m.Map()
	}

	test := func(search string, expect types.Map) {
		actual := index.Search(strings.Split(search, " "))
		a.True(expect.Equals(actual))
	}

	test("foo", getMap(1, 2))
	test("baz", getMap(1, 2, 3))
	test("bar baz", getMap(1))
	test("boo", getMap(3))
	test("blarg", getMap())
}
