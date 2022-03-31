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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nomdl"
	"github.com/dolthub/dolt/go/store/types"
)

func mustHead(ds Dataset) types.Value {
	s, ok := ds.MaybeHead()
	if !ok {
		panic("no head")
	}
	return s
}

func mustHeight(ds Dataset) uint64 {
	h, ok, err := ds.MaybeHeight()
	d.PanicIfError(err)
	d.PanicIfFalse(ok)
	return h
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

func mustStruct(st types.Struct, err error) types.Struct {
	d.PanicIfError(err)
	return st
}

func mustSet(s types.Set, err error) types.Set {
	d.PanicIfError(err)
	return s
}

func mustList(l types.List, err error) types.List {
	d.PanicIfError(err)
	return l
}

func mustMap(m types.Map, err error) types.Map {
	d.PanicIfError(err)
	return m
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

func mustTuple(val types.Tuple, err error) types.Tuple {
	d.PanicIfError(err)
	return val
}

func TestNewCommit(t *testing.T) {
	assert := assert.New(t)

	assertTypeEquals := func(e, a *types.Type) {
		t.Helper()
		assert.True(a.Equals(e), "Actual: %s\nExpected %s\n%s", mustString(a.Describe(context.Background())), mustString(e.Describe(context.Background())), a.HumanReadableString())
	}

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	parentsClosure := mustParentsClosure(t, false)(getParentsClosure(context.Background(), db, parents))
	commit, err := newCommit(context.Background(), types.Float(1), parents, parentsClosure, false, types.EmptyStruct(db.Format()))
	assert.NoError(err)
	at, err := types.TypeOf(commit)
	assert.NoError(err)
	et, err := makeCommitStructType(
		types.EmptyStructType,
		mustType(types.MakeSetType(mustType(types.MakeUnionType()))),
		mustType(types.MakeListType(mustType(types.MakeUnionType()))),
		mustType(types.MakeRefType(types.PrimitiveTypeMap[types.ValueKind])),
		types.PrimitiveTypeMap[types.FloatKind],
		false,
	)
	assert.NoError(err)
	assertTypeEquals(et, at)

	_, err = db.WriteValue(context.Background(), commit)
	assert.NoError(err)

	// Committing another Float
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit, db.Format()))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit2, err := newCommit(context.Background(), types.Float(2), parents, parentsClosure, true, types.EmptyStruct(db.Format()))
	assert.NoError(err)
	at2, err := types.TypeOf(commit2)
	assert.NoError(err)
	et2 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_closure?: Ref<Value>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float,
        }`)
	assertTypeEquals(et2, at2)

	_, err = db.WriteValue(context.Background(), commit2)
	assert.NoError(err)

	// Now commit a String
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, db.Format()))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit3, err := newCommit(context.Background(), types.String("Hi"), parents, parentsClosure, true, types.EmptyStruct(db.Format()))
	assert.NoError(err)
	at3, err := types.TypeOf(commit3)
	assert.NoError(err)
	et3 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_closure?: Ref<Value>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float | String,
        }`)
	assertTypeEquals(et3, at3)

	_, err = db.WriteValue(context.Background(), commit3)
	assert.NoError(err)

	// Now commit a String with MetaInfo
	meta, err := types.NewStruct(db.Format(), "Meta", types.StructData{"date": types.String("some date"), "number": types.Float(9)})
	assert.NoError(err)
	metaType := nomdl.MustParseType(`Struct Meta {
                date: String,
                number: Float,
	}`)
	assertTypeEquals(metaType, mustType(types.TypeOf(meta)))
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, db.Format()))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit4, err := newCommit(context.Background(), types.String("Hi"), parents, parentsClosure, true, meta)
	assert.NoError(err)
	at4, err := types.TypeOf(commit4)
	assert.NoError(err)
	et4 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {} | Struct Meta {
                        date: String,
                        number: Float,
        	},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_closure?: Ref<Value>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float | String,
        }`)
	assertTypeEquals(et4, at4)

	_, err = db.WriteValue(context.Background(), commit4)
	assert.NoError(err)

	// Merge-commit with different parent types
	parents = mustList(types.NewList(context.Background(), db,
		mustRef(types.NewRef(commit2, db.Format())),
		mustRef(types.NewRef(commit3, db.Format()))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit5, err := newCommit(
		context.Background(),
		types.String("Hi"),
		parents,
		parentsClosure,
		true,
		types.EmptyStruct(db.Format()))
	assert.NoError(err)
	at5, err := types.TypeOf(commit5)
	assert.NoError(err)
	et5 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_closure?: Ref<Value>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float | String,
        }`)
	assertTypeEquals(et5, at5)
}

func TestCommitWithoutMetaField(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	defer db.Close()

	metaCommit, err := types.NewStruct(db.Format(), "Commit", types.StructData{
		"value":   types.Float(9),
		"parents": mustSet(types.NewSet(context.Background(), db)),
		"meta":    types.EmptyStruct(db.Format()),
	})
	assert.NoError(err)
	assert.True(IsCommit(metaCommit))

	noMetaCommit, err := types.NewStruct(db.Format(), "Commit", types.StructData{
		"value":   types.Float(9),
		"parents": mustSet(types.NewSet(context.Background(), db)),
	})
	assert.NoError(err)
	assert.False(IsCommit(noMetaCommit))
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

// Convert list of Struct's to List<Ref>
func toRefList(vrw types.ValueReadWriter, commits ...types.Struct) (types.List, error) {
	l, err := types.NewList(context.Background(), vrw)
	if err != nil {
		return types.EmptyList, err
	}

	le := l.Edit()
	for _, p := range commits {
		le = le.Append(mustRef(types.NewRef(p, vrw.Format())))
	}
	return le.List(context.Background())
}

func commonAncWithSetClosure(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader) (a hash.Hash, ok bool, err error) {
	closure, err := NewSetCommitClosure(ctx, vr1, c1)
	if err != nil {
		return hash.Hash{}, false, err
	}
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

func commonAncWithLazyClosure(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader) (a hash.Hash, ok bool, err error) {
	closure := NewLazyCommitClosure(c1, vr1)
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

// Assert that c is the common ancestor of a and b, using multiple common ancestor methods.
func assertCommonAncestor(t *testing.T, expected, a, b types.Value, ldb, rdb *database) {
	type caFinder func(ctx context.Context, c1, c2 *Commit, vr1, vr2 types.ValueReader) (a hash.Hash, ok bool, err error)

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
		t.Run(fmt.Sprintf("%s/%s_%s", name, aref.TargetHash().String(), bref.TargetHash().String()), func(t *testing.T) {
			assert := assert.New(t)
			ctx := context.Background()
			found, ok, err := method(ctx, ac, bc, ldb, rdb)
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
	ds, err = db.Commit(context.Background(), ds, types.String(val), CommitOptions{Parents: mustCommitToTargetHashes(db, parents...)})
	require.NoError(t, err)
	return mustHead(ds), mustHeadAddr(ds)
}

func assertClosureMapValue(t *testing.T, vrw types.ValueReadWriter, v types.Value, h hash.Hash) bool {
	cv, err := vrw.ReadValue(context.Background(), h)
	if !assert.NoError(t, err) {
		return false
	}
	s, ok := cv.(types.Struct)
	if !assert.True(t, ok) {
		return false
	}
	plv, ok, err := s.MaybeGet(parentsListField)
	if !assert.NoError(t, err) {
		return false
	}
	if !assert.True(t, ok) {
		return false
	}
	pl, ok := plv.(types.List)
	if !assert.True(t, ok) {
		return false
	}
	gl, ok := v.(types.List)
	if !assert.True(t, ok) {
		return false
	}
	if !assert.Equal(t, pl.Len(), gl.Len()) {
		return false
	}
	for i := 0; i < int(pl.Len()); i++ {
		piv, err := pl.Get(context.Background(), uint64(i))
		if !assert.NoError(t, err) {
			return false
		}
		giv, err := gl.Get(context.Background(), uint64(i))
		if !assert.NoError(t, err) {
			return false
		}
		pir, ok := piv.(types.Ref)
		if !assert.True(t, ok) {
			return false
		}
		gir, ok := giv.(types.Ref)
		if !assert.True(t, ok) {
			return false
		}
		if !assert.Equal(t, pir.TargetHash(), gir.TargetHash()) {
			return false
		}
	}
	return true
}

func TestCommitParentsClosure(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)

	type expected struct {
		height int
		hash   hash.Hash
	}

	assertCommitParentsClosure := func(v types.Value, es []expected) {
		if _, ok := v.(types.SerialMessage); ok {
			t.Skip("__DOLT_DEV__ does not implement ParentsClosure yet.")
		}
		s, ok := v.(types.Struct)
		if !assert.True(ok) {
			return
		}
		v, ok, err := s.MaybeGet(parentsClosureField)
		if !assert.NoError(err) {
			return
		}
		if len(es) == 0 {
			assert.False(ok, "must not find parents_closure field when its length is 0")
			return
		}
		if !assert.True(ok, "must find parents_closure field in commit.") {
			return
		}
		r, ok := v.(types.Ref)
		if !assert.True(ok, "parents_closure field must contain a ref value.") {
			return
		}
		tv, err := r.TargetValue(context.Background(), db)
		if !assert.NoError(err, "getting target value of parents_closure field must not error") {
			return
		}
		m, ok := tv.(types.Map)
		if !assert.True(ok, "parents_closure ref target value must contain a map value.") {
			return
		}
		if !assert.Equal(len(es), int(m.Len()), "expected length %v and got %v", len(es), m.Len()) {
			return
		}
		i := 0
		err = m.IterAll(context.Background(), func(k, v types.Value) error {
			j := i
			i++
			kt, ok := k.(types.Tuple)
			if !assert.True(ok, "key type must be Tuple") {
				return nil
			}
			if !assert.Equal(2, int(kt.Len()), "key must have length 2") {
				return nil
			}
			hv, err := kt.Get(0)
			if !assert.NoError(err) {
				return nil
			}
			h, ok := hv.(types.Uint)
			if !assert.True(ok, "key first field must be Uint") {
				return nil
			}
			if !assert.Equal(es[j].height, int(uint64(h))) {
				return nil
			}
			hv, err = kt.Get(1)
			if !assert.NoError(err) {
				return nil
			}
			b, ok := hv.(types.InlineBlob)
			if !assert.True(ok, "key second field must be InlineBlob") {
				return nil
			}
			var fh hash.Hash
			copy(fh[:], []byte(b))
			if !assert.Equal(es[j].hash, fh, "hash for idx %d did not match", j) {
				return nil
			}
			assertClosureMapValue(t, db, v, fh)
			return nil
		})
		assert.NoError(err)
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
		{1, a1a},
		{2, a2a},
	})

	assertCommitParentsClosure(b1, []expected{
		{1, a1a},
	})
	assertCommitParentsClosure(b2, []expected{
		{1, a1a},
		{2, b1a},
	})
	assertCommitParentsClosure(b3, []expected{
		{1, a1a},
		{2, b1a},
		{3, b2a},
	})

	assertCommitParentsClosure(c1, []expected{
		{1, a1a},
		{2, a2a},
		{2, b1a},
		{3, a3a},
		{3, b2a},
		{4, b3a},
	})

	assertCommitParentsClosure(d1, []expected{
		{1, a1a},
		{2, a2a},
		{2, b1a},
		{3, a3a},
		{3, b2a},
		{4, b3a},
		{5, c1a},
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

	assertCommonAncestor(t, a1, a1, a1, db, db) // All self
	assertCommonAncestor(t, a1, a1, a2, db, db) // One side self
	assertCommonAncestor(t, a2, a3, b3, db, db) // Common parent
	assertCommonAncestor(t, a2, a4, b4, db, db) // Common grandparent
	assertCommonAncestor(t, a1, a6, c3, db, db) // Traversing multiple parents on both sides

	// No common ancestor
	ctx := context.Background()
	d2c, err := LoadCommitRef(ctx, db, mustRef(types.NewRef(d2, db.Format())))
	require.NoError(t, err)
	a6c, err := LoadCommitRef(ctx, db, mustRef(types.NewRef(a6, db.Format())))
	require.NoError(t, err)
	found, ok, err := FindCommonAncestor(ctx, d2c, a6c, db, db)
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

	assertCommonAncestor(t, a1, a1, a1, db, rdb) // All self
	assertCommonAncestor(t, a1, a1, a2, db, rdb) // One side self
	assertCommonAncestor(t, a2, a3, b3, db, rdb) // Common parent
	assertCommonAncestor(t, a2, a4, b4, db, rdb) // Common grandparent
	assertCommonAncestor(t, a1, a6, c3, db, rdb) // Traversing multiple parents on both sides

	assertCommonAncestor(t, a6, a9, ra9, db, rdb) // Common third parent

	a9c, err := LoadCommitRef(ctx, rdb, mustRef(types.NewRef(a9, rdb.Format())))
	require.NoError(t, err)
	ra9c, err := LoadCommitRef(ctx, db, mustRef(types.NewRef(ra9, db.Format())))
	_, _, err = FindCommonAncestor(context.Background(), ra9c, a9c, rdb, db)
	assert.Error(err)
}

func TestNewCommitRegressionTest(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewViewWithDefaultFormat()).(*database)
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	parentsClosure := mustParentsClosure(t, false)(getParentsClosure(context.Background(), db, parents))
	c1, err := newCommit(context.Background(), types.String("one"), parents, parentsClosure, false, types.EmptyStruct(db.Format()))
	assert.NoError(t, err)
	cx, err := newCommit(context.Background(), types.Bool(true), parents, parentsClosure, false, types.EmptyStruct(db.Format()))
	assert.NoError(t, err)
	_, err = db.WriteValue(context.Background(), c1)
	assert.NoError(t, err)
	_, err = db.WriteValue(context.Background(), cx)
	assert.NoError(t, err)
	value := types.String("two")
	parents, err = types.NewList(context.Background(), db, mustRef(types.NewRef(c1, db.Format())))
	assert.NoError(t, err)
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	meta, err := types.NewStruct(db.Format(), "", types.StructData{
		"basis": cx,
	})
	assert.NoError(t, err)

	// Used to fail
	_, err = newCommit(context.Background(), value, parents, parentsClosure, true, meta)
	assert.NoError(t, err)
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
