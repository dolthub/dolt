// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"fmt"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestNewCommit(t *testing.T) {
	assert := assert.New(t)

	commitFieldNames := []string{MetaField, ParentsField, ValueField}

	assertTypeEquals := func(e, a *types.Type) {
		assert.True(a.Equals(e), "Actual: %s\nExpected %s", a.Describe(), e.Describe())
	}

	commit := NewCommit(types.Number(1), types.NewSet(), types.EmptyStruct)
	at := commit.Type()
	et := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
		types.NumberType,
	})
	assertTypeEquals(et, at)

	// Committing another Number
	commit2 := NewCommit(types.Number(2), types.NewSet(types.NewRef(commit)), types.EmptyStruct)
	at2 := commit2.Type()
	et2 := et
	assertTypeEquals(et2, at2)

	// Now commit a String
	commit3 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2)), types.EmptyStruct)
	at3 := commit3.Type()
	et3 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.EmptyStructType,
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et3, at3)

	// Now commit a String with MetaInfo
	meta := types.NewStruct("Meta", types.StructData{"date": types.String("some date"), "number": types.Number(9)})
	metaType := types.MakeStructType("Meta", []string{"date", "number"}, []*types.Type{types.StringType, types.NumberType})
	assertTypeEquals(metaType, meta.Type())
	commit4 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2)), meta)
	at4 := commit4.Type()
	et4 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		metaType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.MakeUnionType(types.EmptyStructType, metaType),
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et4, at4)

	// Merge-commit with different parent types
	commit5 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2), types.NewRef(commit3)), types.EmptyStruct)
	at5 := commit5.Type()
	et5 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.EmptyStructType,
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et5, at5)
}

func TestCommitWithoutMetaField(t *testing.T) {
	assert := assert.New(t)
	metaCommit := types.NewStruct("Commit", types.StructData{
		"value":   types.Number(9),
		"parents": types.NewSet(),
		"meta":    types.EmptyStruct,
	})
	assert.True(IsCommitType(metaCommit.Type()))

	noMetaCommit := types.NewStruct("Commit", types.StructData{
		"value":   types.Number(9),
		"parents": types.NewSet(),
	})
	assert.False(IsCommitType(noMetaCommit.Type()))
}

// Convert list of Struct's to Set<Ref>
func toRefSet(commits ...types.Struct) types.Set {
	set := types.NewSet()
	for _, p := range commits {
		set = set.Insert(types.NewRef(p))
	}
	return set
}

// Convert Set<Ref<Struct>> to a string of Struct.Get("value")'s
func toValuesString(refSet types.Set, vr types.ValueReader) string {
	values := []string{}
	refSet.IterAll(func(v types.Value) {
		values = append(values, fmt.Sprintf("%v", v.(types.Ref).TargetValue(vr).(types.Struct).Get("value")))
	})
	return strings.Join(values, ",")
}

func TestFindCommonAncestor(t *testing.T) {
	assert := assert.New(t)
	db := NewDatabase(chunks.NewTestStore())
	defer db.Close()

	// Add a commit and return it
	addCommit := func(datasetID string, val string, parents ...types.Struct) types.Struct {
		ds := db.GetDataset(datasetID)
		var err error
		ds, err = db.Commit(ds, types.String(val), CommitOptions{Parents: toRefSet(parents...)})
		assert.NoError(err)
		return ds.Head()
	}

	// Assert that c is the common ancestor of a and b
	assertCommonAncestor := func(expected, a, b types.Struct) {
		if found, ok := FindCommonAncestor(types.NewRef(a), types.NewRef(b), db); assert.True(ok) {
			ancestor := found.TargetValue(db).(types.Struct)
			assert.True(
				expected.Equals(ancestor),
				"%s should be common ancestor of %s, %s. Got %s",
				expected.Get(ValueField),
				a.Get(ValueField),
				b.Get(ValueField),
				ancestor.Get(ValueField),
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
	if found, ok := FindCommonAncestor(types.NewRef(d2), types.NewRef(a6), db); !assert.False(ok) {
		assert.Fail(
			"Unexpected common ancestor!",
			"Should be no common ancestor of %s, %s. Got %s",
			d2.Get(ValueField),
			a6.Get(ValueField),
			found.TargetValue(db).(types.Struct).Get(ValueField),
		)
	}
}
