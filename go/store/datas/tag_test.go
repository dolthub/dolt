// Copyright 2020 Dolthub, Inc.
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

package datas

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/types"
)

func TestNewTag(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()

	assertTypeEquals := func(e, a *types.Type) {
		t.Helper()
		assert.True(a.Equals(e), "Actual: %s\nExpected %s", mustString(a.Describe(ctx)), mustString(e.Describe(ctx)))
	}

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	defer db.Close()

	if db.Format().UsesFlatbuffers() {
		t.Skip()
	}

	parents := mustList(types.NewList(ctx, db))
	parentsClosure := mustParentsClosure(t, false)(getParentsClosure(ctx, db, parents))
	commit, err := newCommit(ctx, types.Float(1), parents, parentsClosure, false, types.EmptyStruct(db.Format()))
	require.NoError(t, err)

	cmRef, err := db.WriteValue(ctx, commit)
	require.NoError(t, err)

	_, tagRef, err := newTag(ctx, db, cmRef.TargetHash(), nil)
	require.NoError(t, err)
	tag, err := tagRef.TargetValue(ctx, db)
	require.NoError(t, err)

	ct, err := makeCommitStructType(
		types.EmptyStructType,
		mustType(types.MakeSetType(mustType(types.MakeUnionType()))),
		mustType(types.MakeListType(mustType(types.MakeUnionType()))),
		mustType(types.MakeRefType(types.PrimitiveTypeMap[types.ValueKind])),
		types.PrimitiveTypeMap[types.FloatKind],
		false,
	)
	require.NoError(t, err)
	et, err := makeTagStructType(
		types.EmptyStructType,
		mustType(types.MakeRefType(ct)),
	)
	require.NoError(t, err)
	at, err := types.TypeOf(tag)
	require.NoError(t, err)

	assertTypeEquals(et, at)
}

func TestPersistedTagConsts(t *testing.T) {
	// changing constants that are persisted requires a migration strategy
	assert.Equal(t, "meta", tagMetaField)
	assert.Equal(t, "ref", tagCommitRefField)
	assert.Equal(t, "Tag", tagName)
}
