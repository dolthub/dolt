// Copyright 2019 Liquidata, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/nomdl"
	"github.com/liquidata-inc/dolt/go/store/types"
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

func TestNewCommit(t *testing.T) {
	assert := assert.New(t)

	assertTypeEquals := func(e, a *types.Type) {
		t.Helper()
		assert.True(a.Equals(e), "Actual: %s\nExpected %s", mustString(a.Describe(context.Background())), mustString(e.Describe(context.Background())))
	}

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	commit, err := NewCommit(context.Background(), types.Float(1), parents, types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at, err := types.TypeOf(commit)
	assert.NoError(err)
	et, err := makeCommitStructType(
		types.EmptyStructType,
		mustType(types.MakeSetType(mustType(types.MakeUnionType()))),
		mustType(types.MakeListType(mustType(types.MakeUnionType()))),
		types.PrimitiveTypeMap[types.FloatKind],
	)
	assert.NoError(err)
	assertTypeEquals(et, at)

	// Committing another Float
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit, types.Format_7_18))))
	commit2, err := NewCommit(context.Background(), types.Float(2), parents, types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at2, err := types.TypeOf(commit2)
	assert.NoError(err)
	et2 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float,
        }`)
	assertTypeEquals(et2, at2)

	// Now commit a String
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, types.Format_7_18))))
	commit3, err := NewCommit(context.Background(), types.String("Hi"), parents, types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at3, err := types.TypeOf(commit3)
	assert.NoError(err)
	et3 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float | String,
        }`)
	assertTypeEquals(et3, at3)

	// Now commit a String with MetaInfo
	meta, err := types.NewStruct(types.Format_7_18, "Meta", types.StructData{"date": types.String("some date"), "number": types.Float(9)})
	assert.NoError(err)
	metaType := nomdl.MustParseType(`Struct Meta {
                date: String,
                number: Float,
	}`)
	assertTypeEquals(metaType, mustType(types.TypeOf(meta)))
	parents = mustList(types.NewList(context.Background(), db, mustRef(types.NewRef(commit2, types.Format_7_18))))
	commit4, err := NewCommit(context.Background(), types.String("Hi"), parents, meta)
	assert.NoError(err)
	at4, err := types.TypeOf(commit4)
	assert.NoError(err)
	et4 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {} | Struct Meta {
                        date: String,
                        number: Float,
        	},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float | String,
        }`)
	assertTypeEquals(et4, at4)

	// Merge-commit with different parent types
	parents = mustList(types.NewList(context.Background(), db,
		mustRef(types.NewRef(commit2, types.Format_7_18)),
		mustRef(types.NewRef(commit3, types.Format_7_18))))
	commit5, err := NewCommit(
		context.Background(),
		types.String("Hi"),
		parents,
		types.EmptyStruct(types.Format_7_18))
	assert.NoError(err)
	at5, err := types.TypeOf(commit5)
	assert.NoError(err)
	et5 := nomdl.MustParseType(`Struct Commit {
                meta: Struct {},
                parents: Set<Ref<Cycle<Commit>>>,
                parents_list: List<Ref<Cycle<Commit>>>,
                value: Float | String,
        }`)
	assertTypeEquals(et5, at5)
}

func TestCommitWithoutMetaField(t *testing.T) {
	assert := assert.New(t)

	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
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

func TestFindCommonAncestor(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	// Add a commit and return it
	addCommit := func(datasetID string, val string, parents ...types.Struct) types.Struct {
		ds, err := db.GetDataset(context.Background(), datasetID)
		assert.NoError(err)
		ds, err = db.Commit(context.Background(), ds, types.String(val), CommitOptions{ParentsList: mustList(toRefList(db, parents...))})
		assert.NoError(err)
		return mustHead(ds)
	}

	// Assert that c is the common ancestor of a and b
	assertCommonAncestor := func(expected, a, b types.Struct) {
		found, ok, err := FindCommonAncestor(context.Background(), mustRef(types.NewRef(a, types.Format_7_18)), mustRef(types.NewRef(b, types.Format_7_18)), db)
		assert.NoError(err)

		if assert.True(ok) {
			tv, err := found.TargetValue(context.Background(), db)
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
	}

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
	a1 := addCommit(a, "a1")
	d1 := addCommit(d, "d1")
	a2 := addCommit(a, "a2", a1)
	c2 := addCommit(c, "c2", a1)
	d2 := addCommit(d, "d2", d1)
	a3 := addCommit(a, "a3", a2)
	b3 := addCommit(b, "b3", a2)
	c3 := addCommit(c, "c3", c2, d2)
	a4 := addCommit(a, "a4", a3)
	b4 := addCommit(b, "b4", b3)
	a5 := addCommit(a, "a5", a4)
	b5 := addCommit(b, "b5", b4, a3)
	a6 := addCommit(a, "a6", a5, b5)

	assertCommonAncestor(a1, a1, a1) // All self
	assertCommonAncestor(a1, a1, a2) // One side self
	assertCommonAncestor(a2, a3, b3) // Common parent
	assertCommonAncestor(a2, a4, b4) // Common grandparent
	assertCommonAncestor(a1, a6, c3) // Traversing multiple parents on both sides

	// No common ancestor
	found, ok, err := FindCommonAncestor(context.Background(), mustRef(types.NewRef(d2, types.Format_7_18)), mustRef(types.NewRef(a6, types.Format_7_18)), db)
	assert.NoError(err)

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
}

func TestNewCommitRegressionTest(t *testing.T) {
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	parents := mustList(types.NewList(context.Background(), db))
	c1, err := NewCommit(context.Background(), types.String("one"), parents, types.EmptyStruct(types.Format_7_18))
	assert.NoError(t, err)
	cx, err := NewCommit(context.Background(), types.Bool(true), parents, types.EmptyStruct(types.Format_7_18))
	assert.NoError(t, err)
	value := types.String("two")
	parents, err = types.NewList(context.Background(), db, mustRef(types.NewRef(c1, types.Format_7_18)))
	assert.NoError(t, err)
	meta, err := types.NewStruct(types.Format_7_18, "", types.StructData{
		"basis": cx,
	})
	assert.NoError(t, err)

	// Used to fail
	_, err = NewCommit(context.Background(), value, parents, meta)
	assert.NoError(t, err)
}

func TestPersistedCommitConsts(t *testing.T) {
	// changing constants that are persisted requires a migration strategy
	assert.Equal(t, "parents", ParentsField)
	assert.Equal(t, "parents_list", ParentsListField)
	assert.Equal(t, "value", ValueField)
	assert.Equal(t, "meta", CommitMetaField)
	assert.Equal(t, "Commit", CommitName)
}
