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

func mustHead(ds Dataset) types.Struct {
	s, ok := ds.MaybeHead()
	if !ok {
		panic("no head")
	}

	return s
}

func mustHeadRef(ds Dataset) types.Ref {
	hr, ok, err := ds.MaybeHeadRef()

	if err != nil {
		panic("error getting head")
	}

	if !ok {
		panic("no head")
	}

	return hr
}

func mustHeadValue(ds Dataset) types.Value {
	val, ok, err := ds.MaybeHeadValue()

	if err != nil {
		panic("error getting head")
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
	db := NewDatabase(storage.NewView()).(*database)
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	parentsClosure := mustParentsClosure(t, false)(getParentsClosure(context.Background(), db, parents))
	commit, err := newCommit(context.Background(), types.Float(1), parents, parentsClosure, false, types.EmptyStruct(types.Format_7_18))
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
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit, types.Format_7_18))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit2, err := newCommit(context.Background(), types.Float(2), parents, parentsClosure, true, types.EmptyStruct(types.Format_7_18))
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
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, types.Format_7_18))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit3, err := newCommit(context.Background(), types.String("Hi"), parents, parentsClosure, true, types.EmptyStruct(types.Format_7_18))
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
	meta, err := types.NewStruct(types.Format_7_18, "Meta", types.StructData{"date": types.String("some date"), "number": types.Float(9)})
	assert.NoError(err)
	metaType := nomdl.MustParseType(`Struct Meta {
                date: String,
                number: Float,
	}`)
	assertTypeEquals(metaType, mustType(types.TypeOf(meta)))
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, types.Format_7_18))))
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
		mustRef(types.NewRef(commit2, types.Format_7_18)),
		mustRef(types.NewRef(commit3, types.Format_7_18))))
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	commit5, err := newCommit(
		context.Background(),
		types.String("Hi"),
		parents,
		parentsClosure,
		true,
		types.EmptyStruct(types.Format_7_18))
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
	db := NewDatabase(storage.NewView()).(*database)
	defer db.Close()

	metaCommit, err := types.NewStruct(types.Format_7_18, "Commit", types.StructData{
		"value":   types.Float(9),
		"parents": mustSet(types.NewSet(context.Background(), db)),
		"meta":    types.EmptyStruct(types.Format_7_18),
	})
	assert.NoError(err)
	assert.True(IsCommit(metaCommit))

	noMetaCommit, err := types.NewStruct(types.Format_7_18, "Commit", types.StructData{
		"value":   types.Float(9),
		"parents": mustSet(types.NewSet(context.Background(), db)),
	})
	assert.NoError(err)
	assert.False(IsCommit(noMetaCommit))
}

// Convert list of Struct's to List<Ref>
func toRefList(vrw types.ValueReadWriter, commits ...types.Struct) (types.List, error) {
	l, err := types.NewList(context.Background(), vrw)
	if err != nil {
		return types.EmptyList, err
	}

	le := l.Edit()
	for _, p := range commits {
		le = le.Append(mustRef(types.NewRef(p, types.Format_7_18)))
	}
	return le.List(context.Background())
}

func commonAncWithSetClosure(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error) {
	closure, err := NewSetRefClosure(ctx, vr1, c1)
	if err != nil {
		return types.Ref{}, false, err
	}
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

func commonAncWithLazyClosure(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error) {
	closure := NewLazyRefClosure(c1, vr1)
	return FindClosureCommonAncestor(ctx, closure, c2, vr2)
}

// Assert that c is the common ancestor of a and b, using multiple common ancestor methods.
func assertCommonAncestor(t *testing.T, expected, a, b types.Struct, ldb, rdb *database) {
	assert := assert.New(t)

	type caFinder func(ctx context.Context, c1, c2 types.Ref, vr1, vr2 types.ValueReader) (a types.Ref, ok bool, err error)

	methods := map[string]caFinder{
		"FindCommonAncestor":                 FindCommonAncestor,
		"SetClosure":                         commonAncWithSetClosure,
		"LazyClosure":                        commonAncWithLazyClosure,
		"FindCommonAncestorUsingParentsList": FindCommonAncestorUsingParentsList,
	}

	for name, method := range methods {
		tn := fmt.Sprintf("find common ancestor using %s", name)
		t.Run(tn, func(t *testing.T) {
			found, ok, err := method(context.Background(), mustRef(types.NewRef(a, types.Format_7_18)), mustRef(types.NewRef(b, types.Format_7_18)), ldb, rdb)
			assert.NoError(err)

			if assert.True(ok) {
				tv, err := found.TargetValue(context.Background(), ldb)
				assert.NoError(err)
				ancestor := tv.(types.Struct)
				expV, _, _ := expected.MaybeGet(ValueField)
				aV, _, _ := a.MaybeGet(ValueField)
				bV, _, _ := b.MaybeGet(ValueField)
				ancV, _, _ := ancestor.MaybeGet(ValueField)
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
func addCommit(t *testing.T, db *database, datasetID string, val string, parents ...types.Struct) (types.Struct, types.Ref) {
	ds, err := db.GetDataset(context.Background(), datasetID)
	assert.NoError(t, err)
	ds, err = db.Commit(context.Background(), ds, types.String(val), CommitOptions{ParentsList: mustList(toRefList(db, parents...))})
	assert.NoError(t, err)
	return mustHead(ds), mustHeadRef(ds)
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
	plv, ok, err := s.MaybeGet(ParentsListField)
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
	db := NewDatabase(storage.NewView()).(*database)

	type expected struct {
		height int
		hash   hash.Hash
	}

	assertCommitParentsClosure := func(s types.Struct, es []expected) {
		v, ok, err := s.MaybeGet(ParentsClosureField)
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
	a1, a1r := addCommit(t, db, a, "a1")
	a2, a2r := addCommit(t, db, a, "a2", a1)
	a3, a3r := addCommit(t, db, a, "a3", a2)

	b1, b1r := addCommit(t, db, b, "b1", a1)
	b2, b2r := addCommit(t, db, b, "b2", b1)
	b3, b3r := addCommit(t, db, b, "b3", b2)

	c1, c1r := addCommit(t, db, c, "c1", a3, b3)

	d1, _ := addCommit(t, db, d, "d1", c1, b3)

	assertCommitParentsClosure(a1, []expected{})
	assertCommitParentsClosure(a2, []expected{
		{1, a1r.TargetHash()},
	})
	assertCommitParentsClosure(a3, []expected{
		{1, a1r.TargetHash()},
		{2, a2r.TargetHash()},
	})

	assertCommitParentsClosure(b1, []expected{
		{1, a1r.TargetHash()},
	})
	assertCommitParentsClosure(b2, []expected{
		{1, a1r.TargetHash()},
		{2, b1r.TargetHash()},
	})
	assertCommitParentsClosure(b3, []expected{
		{1, a1r.TargetHash()},
		{2, b1r.TargetHash()},
		{3, b2r.TargetHash()},
	})

	assertCommitParentsClosure(c1, []expected{
		{1, a1r.TargetHash()},
		{2, a2r.TargetHash()},
		{2, b1r.TargetHash()},
		{3, a3r.TargetHash()},
		{3, b2r.TargetHash()},
		{4, b3r.TargetHash()},
	})

	assertCommitParentsClosure(d1, []expected{
		{1, a1r.TargetHash()},
		{2, a2r.TargetHash()},
		{2, b1r.TargetHash()},
		{3, a3r.TargetHash()},
		{3, b2r.TargetHash()},
		{4, b3r.TargetHash()},
		{5, c1r.TargetHash()},
	})
}

func TestFindCommonAncestor(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView()).(*database)

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
	found, ok, err := FindCommonAncestor(context.Background(), mustRef(types.NewRef(d2, types.Format_7_18)), mustRef(types.NewRef(a6, types.Format_7_18)), db, db)
	require.NoError(t, err)

	if !assert.False(ok) {
		d2V, _, _ := d2.MaybeGet(ValueField)
		a6V, _, _ := a6.MaybeGet(ValueField)
		fTV, _ := found.TargetValue(context.Background(), db)
		fV, _, _ := fTV.(types.Struct).MaybeGet(ValueField)

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
	db = NewDatabase(storage.NewView()).(*database)
	defer db.Close()

	rstorage := &chunks.TestStorage{}
	rdb := NewDatabase(rstorage.NewView()).(*database)
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

	_, _, err = FindCommonAncestor(context.Background(), mustRef(types.NewRef(a9, types.Format_7_18)), mustRef(types.NewRef(ra9, types.Format_7_18)), rdb, db)
	assert.Error(err)
}

func TestNewCommitRegressionTest(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView()).(*database)
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	parentsClosure := mustParentsClosure(t, false)(getParentsClosure(context.Background(), db, parents))
	c1, err := newCommit(context.Background(), types.String("one"), parents, parentsClosure, false, types.EmptyStruct(types.Format_7_18))
	assert.NoError(t, err)
	cx, err := newCommit(context.Background(), types.Bool(true), parents, parentsClosure, false, types.EmptyStruct(types.Format_7_18))
	assert.NoError(t, err)
	_, err = db.WriteValue(context.Background(), c1)
	assert.NoError(t, err)
	_, err = db.WriteValue(context.Background(), cx)
	assert.NoError(t, err)
	value := types.String("two")
	parents, err = types.NewList(context.Background(), db, mustRef(types.NewRef(c1, types.Format_7_18)))
	assert.NoError(t, err)
	parentsClosure = mustParentsClosure(t, true)(getParentsClosure(context.Background(), db, parents))
	meta, err := types.NewStruct(types.Format_7_18, "", types.StructData{
		"basis": cx,
	})
	assert.NoError(t, err)

	// Used to fail
	_, err = newCommit(context.Background(), value, parents, parentsClosure, true, meta)
	assert.NoError(t, err)
}

func TestPersistedCommitConsts(t *testing.T) {
	// changing constants that are persisted requires a migration strategy
	assert.Equal(t, "parents", ParentsField)
	assert.Equal(t, "parents_list", ParentsListField)
	assert.Equal(t, "parents_closure", ParentsClosureField)
	assert.Equal(t, "value", ValueField)
	assert.Equal(t, "meta", CommitMetaField)
	assert.Equal(t, "Commit", CommitName)
}
