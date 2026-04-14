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

package datas

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func mustHead(ds Dataset) types.Value {
	s, ok := ds.MaybeHead()
	if !ok {
		panic("no head")
	}
	return s
}

func mustHeadValue(ds Dataset) types.Value {
	val, ok, err := ds.MaybeHeadValue()
	if err != nil {
		panic("error getting head " + err.Error())
	}
	if !ok {
		panic("no head")
	}
	return val
}

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

func mustParentsClosure(t *testing.T, exists bool) func(types.Ref, bool, error) types.Ref {
	return func(r types.Ref, got bool, err error) types.Ref {
		t.Helper()
		require.NoError(t, err)
		require.Equal(t, exists, got)
		return r
	}
}

func mustType(t *types.Type, err error) *types.Type {
	d.PanicIfError(err)
	return t
}

func mustRef(ref types.Ref, err error) types.Ref {
	d.PanicIfError(err)
	return ref
}

func mustValue(val types.Value, err error) types.Value {
	d.PanicIfError(err)
	return val
}

func TestIsCommit(t *testing.T) {
	assert := assert.New(t)

	meta, err := NewCommitMetaWithAuthorDate("test", "test@test.com", "test commit", time.UnixMilli(0))
	if err != nil {
		t.Fatal(err)
	}

	commitMsg, _ := commit_flatbuffer(hash.Hash{}, CommitOptions{
		Meta: meta,
	}, nil, hash.Hash{})
	metaCommit := types.SerialMessage(commitMsg)
	ok, err := IsCommit(metaCommit)
	assert.NoError(err)
	assert.True(ok)

	rvBuilder := flatbuffers.NewBuilder(64)
	serial.RootValueStart(rvBuilder)
	notACommit := types.SerialMessage(serial.FinishMessage(
		rvBuilder,
		serial.RootValueEnd(rvBuilder),
		[]byte(serial.RootValueFileID),
	))
	ok, err = IsCommit(notACommit)
	assert.NoError(err)
	assert.False(ok)
}

func mustCommitToTargetHashes(vrw types.ValueReadWriter, commits ...types.Value) []hash.Hash {
	ret := make([]hash.Hash, len(commits))
	for i, c := range commits {
		r, err := types.NewRef(c, vrw.Format())
		if err != nil {
			panic(err)
		}
		ret[i] = r.TargetHash()
	}
	return ret
}

func commonAncWithSetClosure(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader, ns1, ns2 tree.NodeStore) (a hash.Hash, ok bool, err error) {
	closure, err := NewSetCommitClosure(ctx, vr1, c1)
	if err != nil {
		return hash.Hash{}, false, err
	}
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

func commonAncWithLazyClosure(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader, ns1, ns2 tree.NodeStore) (a hash.Hash, ok bool, err error) {
	closure := NewLazyCommitClosure(c1, vr1)
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

// Assert that c is the common ancestor of a and b, using multiple common ancestor methods.
func assertCommonAncestor(t *testing.T, expected, a, b types.Value, ldb, rdb *database, desc string) {
	type caFinder func(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader, ns1, ns2 tree.NodeStore) (a hash.Hash, ok bool, err error)

	methods := map[string]caFinder{
		"FindCommonAncestor":                 FindCommonAncestor,
		"SetClosure":                         commonAncWithSetClosure,
		"LazyClosure":                        commonAncWithLazyClosure,
		"FindCommonAncestorUsingParentsList": findCommonAncestorUsingParentsList,
	}

	aref := mustRef(types.NewRef(a, ldb.Format()))
	bref := mustRef(types.NewRef(b, rdb.Format()))
	ac, err := LoadCommitRef(context.Background(), ldb, aref)
	require.NoError(t, err)
	bc, err := LoadCommitRef(context.Background(), rdb, bref)
	require.NoError(t, err)

	for name, method := range methods {
		t.Run(fmt.Sprintf("%s/%s", name, desc), func(t *testing.T) {
			assert := assert.New(t)
			ctx := context.Background()
			found, ok, err := method(ctx, ac, bc, ldb, rdb, ldb.ns, rdb.ns)
			assert.NoError(err)
			if assert.True(ok) {
				tv, err := ldb.ReadValue(context.Background(), found)
				assert.NoError(err)
				ancestor := tv
				expV, _ := GetCommittedValue(ctx, ldb, expected)
				aV, _ := GetCommittedValue(ctx, ldb, a)
				bV, _ := GetCommittedValue(ctx, rdb, b)
				ancV, _ := GetCommittedValue(ctx, ldb, ancestor)
				assert.True(
					expected.Equals(ancestor),
					"%s should be common ancestor of %s, %s. Got %s",
					expV,
					aV,
					bV,
					ancV,
				)
			}
		})
	}
}

// Add a commit and return it.
func addCommit(t *testing.T, db *database, datasetID string, val string, parents ...types.Value) (types.Value, hash.Hash) {
	ds, err := db.GetDataset(context.Background(), datasetID)
	assert.NoError(t, err)
	epoch := CommitDateAt(time.UnixMilli(0))
	meta := &CommitMeta{Author: CommitIdent{Date: epoch}, Committer: CommitIdent{Date: epoch}}
	ds, err = db.Commit(context.Background(), ds, types.String(val), CommitOptions{Parents: mustCommitToTargetHashes(db, parents...), Meta: meta})
	require.NoError(t, err)
	return mustHead(ds), mustHeadAddr(ds)
}

func TestCommitParentsClosure(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	ctx := context.Background()

	type expected struct {
		height int
		hash   hash.Hash
	}

	assertCommitParentsClosure := func(v types.Value, es []expected) {
		sort.Slice(es, func(i, j int) bool {
			if es[i].height == es[j].height {
				return bytes.Compare(es[i].hash[:], es[j].hash[:]) > 0
			}
			return es[i].height > es[j].height
		})
		c, err := commitPtr(db.Format(), v, nil)
		if !assert.NoError(err) {
			return
		}
		iter, err := newParentsClosureIterator(ctx, c, db, db.ns)
		if !assert.NoError(err) {
			return
		}
		if len(es) == 0 {
			assert.Nil(iter)
			return
		}
		for _, e := range es {
			if !assert.True(iter.Next(ctx)) {
				return
			}
			if !assert.Equal(e.hash, iter.Hash()) {
				return
			}
			if !assert.Equal(uint64(e.height), iter.Height()) {
				return
			}
		}
		assert.False(iter.Next(ctx))
		assert.NoError(iter.Err())
	}

	a, b, c, d := "ds-a", "ds-b", "ds-c", "ds-d"
	a1, a1a := addCommit(t, db, a, "a1")
	a2, a2a := addCommit(t, db, a, "a2", a1)
	a3, a3a := addCommit(t, db, a, "a3", a2)

	b1, b1a := addCommit(t, db, b, "b1", a1)
	b2, b2a := addCommit(t, db, b, "b2", b1)
	b3, b3a := addCommit(t, db, b, "b3", b2)

	c1, c1a := addCommit(t, db, c, "c1", a3, b3)

	d1, _ := addCommit(t, db, d, "d1", c1, b3)

	assertCommitParentsClosure(a1, []expected{})
	assertCommitParentsClosure(a2, []expected{
		{1, a1a},
	})
	assertCommitParentsClosure(a3, []expected{
		{2, a2a},
		{1, a1a},
	})

	assertCommitParentsClosure(b1, []expected{
		{1, a1a},
	})
	assertCommitParentsClosure(b2, []expected{
		{2, b1a},
		{1, a1a},
	})
	assertCommitParentsClosure(b3, []expected{
		{3, b2a},
		{2, b1a},
		{1, a1a},
	})

	assertCommitParentsClosure(c1, []expected{
		{4, b3a},
		{3, b2a},
		{3, a3a},
		{2, b1a},
		{2, a2a},
		{1, a1a},
	})

	assertCommitParentsClosure(d1, []expected{
		{5, c1a},
		{4, b3a},
		{3, b2a},
		{3, a3a},
		{2, b1a},
		{2, a2a},
		{1, a1a},
	})
}

func TestFindCommonAncestor(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)

	// Build commit DAG
	//
	// ds-a: a1<-a2<-a3<-a4<-a5<-a6
	//       ^    ^   ^          |
	//       |     \   \----\  /-/
	//       |      \        \V
	// ds-b:  \      b3<-b4<-b5
	//         \
	//          \
	// ds-c:     c2<-c3
	//              /
	//             /
	//            V
	// ds-d: d1<-d2
	//
	a, b, c, d := "ds-a", "ds-b", "ds-c", "ds-d"
	a1, _ := addCommit(t, db, a, "a1")
	d1, _ := addCommit(t, db, d, "d1")
	a2, _ := addCommit(t, db, a, "a2", a1)
	c2, _ := addCommit(t, db, c, "c2", a1)
	d2, _ := addCommit(t, db, d, "d2", d1)
	a3, _ := addCommit(t, db, a, "a3", a2)
	b3, _ := addCommit(t, db, b, "b3", a2)
	c3, _ := addCommit(t, db, c, "c3", c2, d2)
	a4, _ := addCommit(t, db, a, "a4", a3)
	b4, _ := addCommit(t, db, b, "b4", b3)
	a5, _ := addCommit(t, db, a, "a5", a4)
	b5, _ := addCommit(t, db, b, "b5", b4, a3)
	a6, _ := addCommit(t, db, a, "a6", a5, b5)

	assertCommonAncestor(t, a1, a1, a1, db, db, "all self")
	assertCommonAncestor(t, a1, a1, a2, db, db, "one side self")
	assertCommonAncestor(t, a2, a3, b3, db, db, "common parent")
	assertCommonAncestor(t, a2, a4, b4, db, db, "common grandeparent")
	assertCommonAncestor(t, a1, a6, c3, db, db, "traversing multiple parents on both sides")

	// No common ancestor
	ctx := context.Background()
	d2c, err := LoadCommitRef(ctx, db, mustRef(types.NewRef(d2, db.Format())))
	require.NoError(t, err)
	a6c, err := LoadCommitRef(ctx, db, mustRef(types.NewRef(a6, db.Format())))
	require.NoError(t, err)
	found, ok, err := FindCommonAncestor(ctx, d2c, a6c, db, db, db.ns, db.ns)
	require.NoError(t, err)

	if !assert.False(ok) {
		d2V, _ := GetCommittedValue(ctx, db, d2)
		a6V, _ := GetCommittedValue(ctx, db, a6)
		fTV, _ := db.ReadValue(ctx, found)
		fV, _ := GetCommittedValue(ctx, db, fTV)

		assert.Fail(
			"Unexpected common ancestor!",
			"Should be no common ancestor of %s, %s. Got %s",
			d2V,
			a6V,
			fV,
		)
	}

	assert.NoError(db.Close())

	t.Run("DifferentVRWs", func(t *testing.T) {
		storage = &chunks.TestStorage{}
		db = NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
		defer db.Close()

		rstorage := &chunks.TestStorage{}
		rdb := NewDatabase(rstorage.NewViewWithDefaultFormat()).(*database)
		defer rdb.Close()

		// Rerun the tests when using two difference Databases for left and
		// right commits. Both databases have all the previous commits.
		a, b, c, d = "ds-a", "ds-b", "ds-c", "ds-d"
		a1, _ = addCommit(t, db, a, "a1")
		d1, _ = addCommit(t, db, d, "d1")
		a2, _ = addCommit(t, db, a, "a2", a1)
		c2, _ = addCommit(t, db, c, "c2", a1)
		d2, _ = addCommit(t, db, d, "d2", d1)
		a3, _ = addCommit(t, db, a, "a3", a2)
		b3, _ = addCommit(t, db, b, "b3", a2)
		c3, _ = addCommit(t, db, c, "c3", c2, d2)
		a4, _ = addCommit(t, db, a, "a4", a3)
		b4, _ = addCommit(t, db, b, "b4", b3)
		a5, _ = addCommit(t, db, a, "a5", a4)
		b5, _ = addCommit(t, db, b, "b5", b4, a3)
		a6, _ = addCommit(t, db, a, "a6", a5, b5)

		addCommit(t, rdb, a, "a1")
		addCommit(t, rdb, d, "d1")
		addCommit(t, rdb, a, "a2", a1)
		addCommit(t, rdb, c, "c2", a1)
		addCommit(t, rdb, d, "d2", d1)
		addCommit(t, rdb, a, "a3", a2)
		addCommit(t, rdb, b, "b3", a2)
		addCommit(t, rdb, c, "c3", c2, d2)
		addCommit(t, rdb, a, "a4", a3)
		addCommit(t, rdb, b, "b4", b3)
		addCommit(t, rdb, a, "a5", a4)
		addCommit(t, rdb, b, "b5", b4, a3)
		addCommit(t, rdb, a, "a6", a5, b5)

		// Additionally, |db| has a6<-a7<-a8<-a9.
		// |rdb| has a6<-ra7<-ra8<-ra9.
		a7, _ := addCommit(t, db, a, "a7", a6)
		a8, _ := addCommit(t, db, a, "a8", a7)
		a9, _ := addCommit(t, db, a, "a9", a8)

		ra7, _ := addCommit(t, rdb, a, "ra7", a6)
		ra8, _ := addCommit(t, rdb, a, "ra8", ra7)
		ra9, _ := addCommit(t, rdb, a, "ra9", ra8)

		assertCommonAncestor(t, a1, a1, a1, db, rdb, "all self")
		assertCommonAncestor(t, a1, a1, a2, db, rdb, "one side self")
		assertCommonAncestor(t, a2, a3, b3, db, rdb, "common parent")
		assertCommonAncestor(t, a2, a4, b4, db, rdb, "common grandeparent")
		assertCommonAncestor(t, a1, a6, c3, db, rdb, "traversing multiple parents on both sides")

		assertCommonAncestor(t, a6, a9, ra9, db, rdb, "common third parent")

		a9c, err := CommitFromValue(db.Format(), a9)
		require.NoError(t, err)
		ra9c, err := CommitFromValue(rdb.Format(), ra9)
		require.NoError(t, err)
		_, _, err = FindCommonAncestor(context.Background(), ra9c, a9c, db, rdb, db.ns, rdb.ns)
		assert.Error(err)
	})
}

func TestPersistedCommitConsts(t *testing.T) {
	// changing constants that are persisted requires a migration strategy
	assert.Equal(t, "parents", parentsField)
	assert.Equal(t, "parents_list", parentsListField)
	assert.Equal(t, "parents_closure", parentsClosureField)
	assert.Equal(t, "value", valueField)
	assert.Equal(t, "meta", commitMetaField)
	assert.Equal(t, "Commit", commitName)
}
