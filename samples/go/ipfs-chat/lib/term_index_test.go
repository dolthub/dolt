// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package lib

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestRun(t *testing.T) {
	a := assert.New(t)

	docs := []struct {
		terms string
		id    int
	}{
		{"foo bar baz", 1},
		{"foo baz", 2},
		{"baz bat boo", 3},
	}

	indexEditor := NewTermIndex(types.NewMap()).Edit()
	for _, doc := range docs {
		indexEditor.InsertAll(strings.Split(doc.terms, " "), types.Number(doc.id))
	}

	index := indexEditor.Value(nil)

	getMap := func(keys ...int) types.Map {
		m := types.NewMap().Edit()
		for _, k := range keys {
			m.Set(types.Number(k), types.Bool(true))
		}
		return m.Map(nil)
	}

	tc := []struct {
		search string
		expect types.Map
	}{
		{"foo", getMap(1, 2)},
		{"baz", getMap(1, 2, 3)},
		{"bar baz", getMap(1)},
		{"boo", getMap(3)},
		{"blarg", getMap()},
	}

	for _, c := range tc {
		actual := index.Search(strings.Split(c.search, " "))
		a.True(c.expect.Equals(actual))
	}
}
