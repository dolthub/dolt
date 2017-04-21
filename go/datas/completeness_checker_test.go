// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestCompletenessChecker(t *testing.T) {
	storage := &chunks.TestStorage{}
	b := types.Bool(true)
	r := types.NewRef(b)

	t.Run("Panic", func(t *testing.T) {
		badRef := types.NewRef(types.Number(42))
		t.Run("AllBad", func(t *testing.T) {
			t.Parallel()
			cc := newCompletenessChecker()
			cc.AddRefs(badRef)
			cc.AddRefs(r)
			assert.Panics(t, func() { cc.PanicIfDangling(storage.NewView()) })
		})
		t.Run("SomeBad", func(t *testing.T) {
			t.Parallel()
			cs := storage.NewView()
			cs.Put(types.EncodeValue(b, nil))

			cc := newCompletenessChecker()
			cc.AddRefs(badRef)
			cc.AddRefs(r)
			assert.Panics(t, func() { cc.PanicIfDangling(cs) })
		})
	})

	t.Run("Success", func(t *testing.T) {
		t.Run("PendingChunk", func(t *testing.T) {
			t.Parallel()
			cs := storage.NewView()
			cs.Put(types.EncodeValue(b, nil))

			cc := newCompletenessChecker()
			cc.AddRefs(r)
			assert.NotPanics(t, func() { cc.PanicIfDangling(cs) })
		})
		t.Run("ExistingChunk", func(t *testing.T) {
			t.Parallel()
			cs := storage.NewView()
			cs.Put(types.EncodeValue(b, nil))

			cc := newCompletenessChecker()
			cc.AddRefs(r)
			assert.NotPanics(t, func() { cc.PanicIfDangling(cs) })
		})
	})
}
