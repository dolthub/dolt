package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

// testing strategy
// - test simplifying each kind in isolation, both shallow and deep
// - test merging structs in isolation, both shallow and deep
// - test makeSupertype
//   - pass one type only
//   - test that instances are properly deduplicated
//   - test union flattening
//   - test grouping of the various kinds
//   - test cycles
// - test makeMergedType
//   - test structs and structs nested in collections

func TestSimplifyHelpers(t *testing.T) {
	structSimplifier := func(n string) func(typeset, bool) *Type {
		return func(ts typeset, merge bool) *Type {
			return simplifyStructs(n, ts, merge)
		}
	}

	cases := []struct {
		f   func(typeset, bool) *Type
		in  []*Type
		out *Type
	}{
		// ref<bool> -> ref<bool>
		{simplifyRefs,
			[]*Type{MakeRefType(BoolType)},
			MakeRefType(BoolType)},
		// ref<number>|ref<string>|ref<blob> -> ref<number|string|blob>
		{simplifyRefs,
			[]*Type{MakeRefType(NumberType), MakeRefType(StringType), MakeRefType(BlobType)},
			MakeRefType(MakeUnionType(NumberType, StringType, BlobType))},
		// ref<set<bool>>|ref<set<string>> -> ref<set<bool|string>>
		{simplifyRefs,
			[]*Type{MakeRefType(MakeSetType(BoolType)), MakeRefType(MakeSetType(StringType))},
			MakeRefType(MakeSetType(MakeUnionType(BoolType, StringType)))},
		// ref<set<bool>|ref<set<string>>|ref<number> -> ref<set<bool|string>|number>
		{simplifyRefs,
			[]*Type{MakeRefType(MakeSetType(BoolType)), MakeRefType(MakeSetType(StringType)), MakeRefType(NumberType)},
			MakeRefType(MakeUnionType(MakeSetType(MakeUnionType(BoolType, StringType)), NumberType))},

		// set<bool> -> set<bool>
		{simplifySets,
			[]*Type{MakeSetType(BoolType)},
			MakeSetType(BoolType)},
		// set<number>|set<string>|set<blob> -> set<number|string|blob>
		{simplifySets,
			[]*Type{MakeSetType(NumberType), MakeSetType(StringType), MakeSetType(BlobType)},
			MakeSetType(MakeUnionType(NumberType, StringType, BlobType))},
		// set<set<bool>>|set<set<string>> -> set<set<bool|string>>
		{simplifySets,
			[]*Type{MakeSetType(MakeSetType(BoolType)), MakeSetType(MakeSetType(StringType))},
			MakeSetType(MakeSetType(MakeUnionType(BoolType, StringType)))},
		// set<set<bool>|set<set<string>>|set<number> -> set<set<bool|string>|number>
		{simplifySets,
			[]*Type{MakeSetType(MakeSetType(BoolType)), MakeSetType(MakeSetType(StringType)), MakeSetType(NumberType)},
			MakeSetType(MakeUnionType(MakeSetType(MakeUnionType(BoolType, StringType)), NumberType))},

		// list<bool> -> list<bool>
		{simplifyLists,
			[]*Type{MakeListType(BoolType)},
			MakeListType(BoolType)},
		// list<number>|list<string>|list<blob> -> list<number|string|blob>
		{simplifyLists,
			[]*Type{MakeListType(NumberType), MakeListType(StringType), MakeListType(BlobType)},
			MakeListType(MakeUnionType(NumberType, StringType, BlobType))},
		// list<set<bool>>|list<set<string>> -> list<set<bool|string>>
		{simplifyLists,
			[]*Type{MakeListType(MakeListType(BoolType)), MakeListType(MakeListType(StringType))},
			MakeListType(MakeListType(MakeUnionType(BoolType, StringType)))},
		// list<set<bool>|list<set<string>>|list<number> -> list<set<bool|string>|number>
		{simplifyLists,
			[]*Type{MakeListType(MakeListType(BoolType)), MakeListType(MakeListType(StringType)), MakeListType(NumberType)},
			MakeListType(MakeUnionType(MakeListType(MakeUnionType(BoolType, StringType)), NumberType))},

		// map<bool,bool> -> map<bool,bool>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType)},
			MakeMapType(BoolType, BoolType)},
		// map<bool,bool>|map<bool,string> -> map<bool,bool|string>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType), MakeMapType(BoolType, StringType)},
			MakeMapType(BoolType, MakeUnionType(BoolType, StringType))},
		// map<bool,bool>|map<string,bool> -> map<bool|string,bool>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType), MakeMapType(StringType, BoolType)},
			MakeMapType(MakeUnionType(BoolType, StringType), BoolType)},
		// map<bool,bool>|map<string,string> -> map<bool|string,bool|string>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType), MakeMapType(StringType, StringType)},
			MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, StringType))},
		// map<set<bool>,bool>|map<set<string>,string> -> map<set<bool|string>,bool|string>
		{simplifyMaps,
			[]*Type{MakeMapType(MakeSetType(BoolType), BoolType), MakeMapType(MakeSetType(StringType), StringType)},
			MakeMapType(MakeSetType(MakeUnionType(BoolType, StringType)), MakeUnionType(BoolType, StringType))},

		// struct{foo:bool} -> struct{foo:bool}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
			MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
		// struct{foo:bool}|struct{foo:number} -> struct{foo:bool|number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeUnionType(BoolType, StringType)})},
		// struct{foo:bool}|struct{foo:bool,bar:number} -> struct{foo:bool}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"foo": BoolType, "bar": NumberType})},
			MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
		// struct{foo:bool}|struct{bar:number} -> struct{}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"bar": NumberType})},
			MakeStructTypeFromFields("", FieldMap{})},
		// struct{foo:ref<bool>}|struct{foo:ref<number>} -> struct{foo:ref<bool|number>}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(BoolType)}),
				MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(NumberType)})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(MakeUnionType(BoolType, NumberType))})},

		// struct A{foo:bool}|struct A{foo:string} -> struct A{foo:bool|string}
		{structSimplifier("A"),
			[]*Type{MakeStructTypeFromFields("A", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("A", FieldMap{"foo": MakeUnionType(BoolType, StringType)})},

		// map<string, struct A{foo:string}>,  map<string, struct A{foo:string, bar:bool}>
		// 	-> map<string, struct A{foo:string}>
		{simplifyMaps,
			[]*Type{MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType})),
				MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType, "bar": BoolType})),
			},
			MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType}))},
	}

	for i, c := range cases {
		act := c.f(newTypeset(c.in...), false /*don't merge*/)
		assert.True(t, c.out.Equals(act), "Test case as position %d - got %s", i, act.Describe())
	}
}

func TestMergeHelpers(t *testing.T) {
	structSimplifier := func(n string) func(typeset, bool) *Type {
		return func(ts typeset, merge bool) *Type {
			return simplifyStructs(n, ts, merge)
		}
	}

	cases := []struct {
		f   func(typeset, bool) *Type
		in  []*Type
		out *Type
	}{
		// struct{foo:bool} -> struct{foo:bool}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
			MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
		// struct{foo:bool}|struct{foo:number} -> struct{foo:bool|number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeUnionType(BoolType, StringType)})},
		// struct{foo:bool}|struct{foo:bool,bar:number} -> struct{foo:bool,bar:number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"foo": BoolType, "bar": NumberType})},
			MakeStructTypeFromFields("", FieldMap{"foo": BoolType, "bar": NumberType})},
		// struct{foo:bool}|struct{bar:number} -> struct{foo:bool,bar:number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"bar": NumberType})},
			MakeStructTypeFromFields("", FieldMap{"foo": BoolType, "bar": NumberType})},
		// struct{foo:ref<bool>}|struct{foo:ref<number>} -> struct{foo:ref<bool|number>}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(BoolType)}),
				MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(NumberType)})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(MakeUnionType(BoolType, NumberType))})},

		// struct A{foo:bool}|struct A{foo:string} -> struct A{foo:bool|string}
		{structSimplifier("A"),
			[]*Type{MakeStructTypeFromFields("A", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("A", FieldMap{"foo": MakeUnionType(BoolType, StringType)})},

		// map<string, struct A{foo:string}>,  map<string, struct A{bar:bool}>
		// 	-> map<string, struct A{foo:string, bar:bool}>
		{simplifyMaps,
			[]*Type{MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType})),
				MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"bar": BoolType})),
			},
			MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType, "bar": BoolType}))},
	}

	for i, c := range cases {
		act := c.f(newTypeset(c.in...), true /* merge */)
		assert.True(t, c.out.Equals(act), "Test case as position %d - got %s", i, act.Describe())
	}
}

func TestMakeSimplifiedUnion(t *testing.T) {
	cycleType := MakeStructTypeFromFields("", FieldMap{"self": MakeCycleType(0)})

	// TODO: Why is this first step necessary?
	cycleType = ToUnresolvedType(cycleType)
	cycleType = resolveStructCycles(cycleType, nil)

	cases := []struct {
		in  []*Type
		out *Type
	}{
		// {} -> <empty-union>
		{[]*Type{},
			MakeUnionType()},
		// {bool} -> bool
		{[]*Type{BoolType},
			BoolType},
		// {bool,bool} -> bool
		{[]*Type{BoolType, BoolType},
			BoolType},
		// {bool,number} -> bool|number
		{[]*Type{BoolType, NumberType},
			MakeUnionType(BoolType, NumberType)},
		// {bool,number|(string|blob|number)} -> bool|number|string|blob
		{[]*Type{BoolType, MakeUnionType(NumberType, MakeUnionType(StringType, BlobType, NumberType))},
			MakeUnionType(BoolType, NumberType, StringType, BlobType)},

		// {ref<number>} -> ref<number>
		{[]*Type{MakeRefType(NumberType)},
			MakeRefType(NumberType)},
		// {ref<number>,ref<string>} -> ref<number|string>
		{[]*Type{MakeRefType(NumberType), MakeRefType(StringType)},
			MakeRefType(MakeUnionType(NumberType, StringType))},

		// {set<number>} -> set<number>
		{[]*Type{MakeSetType(NumberType)},
			MakeSetType(NumberType)},
		// {set<number>,set<string>} -> set<number|string>
		{[]*Type{MakeSetType(NumberType), MakeSetType(StringType)},
			MakeSetType(MakeUnionType(NumberType, StringType))},

		// {list<number>} -> list<number>
		{[]*Type{MakeListType(NumberType)},
			MakeListType(NumberType)},
		// {list<number>,list<string>} -> list<number|string>
		{[]*Type{MakeListType(NumberType), MakeListType(StringType)},
			MakeListType(MakeUnionType(NumberType, StringType))},

		// {map<number,number>} -> map<number,number>
		{[]*Type{MakeMapType(NumberType, NumberType)},
			MakeMapType(NumberType, NumberType)},
		// {map<number,number>,map<string,string>} -> map<number|string,number|string>
		{[]*Type{MakeMapType(NumberType, NumberType), MakeMapType(StringType, StringType)},
			MakeMapType(MakeUnionType(NumberType, StringType), MakeUnionType(NumberType, StringType))},

		// {struct{foo:number}} -> struct{foo:number}
		{[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": NumberType})},
			MakeStructTypeFromFields("", FieldMap{"foo": NumberType})},
		// {struct{foo:number}, struct{foo:string}} -> struct{foo:number|string}
		{[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": NumberType}),
			MakeStructTypeFromFields("", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeUnionType(NumberType, StringType)})},

		// {bool,string,ref<bool>,ref<string>,ref<set<string>>,ref<set<bool>>,
		//    struct{foo:number},struct{bar:string},struct A{foo:number}} ->
		// bool|string|ref<bool|string|set<string|bool>>|struct{}|struct A{foo:number}
		{
			[]*Type{
				BoolType, StringType,
				MakeRefType(BoolType), MakeRefType(StringType),
				MakeRefType(MakeSetType(BoolType)), MakeRefType(MakeSetType(StringType)),
				MakeStructTypeFromFields("", FieldMap{"foo": NumberType}),
				MakeStructTypeFromFields("", FieldMap{"bar": StringType}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType}),
			},
			MakeUnionType(
				BoolType, StringType,
				MakeRefType(MakeUnionType(BoolType, StringType,
					MakeSetType(MakeUnionType(BoolType, StringType)))),
				MakeStructTypeFromFields("", FieldMap{}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType}),
			),
		},

		{[]*Type{cycleType}, cycleType},

		{[]*Type{cycleType, NumberType, StringType},
			MakeUnionType(cycleType, NumberType, StringType)},
	}

	for i, c := range cases {
		act := makeSimplifiedType(c.in...)
		assert.True(t, c.out.Equals(act), "Test case as position %d - got %s, expected %s", i, act.Describe(), c.out.Describe())
	}
}

func TestMakeMergedType(t *testing.T) {
	cycleType := MakeStructTypeFromFields("", FieldMap{"self": MakeCycleType(0)})

	// TODO: Why is this first step necessary?
	cycleType = ToUnresolvedType(cycleType)
	cycleType = resolveStructCycles(cycleType, nil)

	cases := []struct {
		in  []*Type
		out *Type
	}{
		// {struct{foo:number}} -> struct{foo:number}
		{[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": NumberType})},
			MakeStructTypeFromFields("", FieldMap{"foo": NumberType})},
		// {struct{foo:number}, struct{foo:string}} -> struct{foo:number|string}
		{[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": NumberType}),
			MakeStructTypeFromFields("", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeUnionType(NumberType, StringType)})},

		// {bool,string,ref<bool>,ref<string>,ref<set<string>>,ref<set<bool>>,
		//    struct{foo:number},struct{bar:string},struct A{foo:number}} ->
		// bool|string|ref<bool|string|set<string|bool>>|struct{}|struct A{foo:number}
		{
			[]*Type{
				BoolType, StringType,
				MakeRefType(BoolType), MakeRefType(StringType),
				MakeRefType(MakeSetType(BoolType)), MakeRefType(MakeSetType(StringType)),
				MakeStructTypeFromFields("", FieldMap{"foo": NumberType}),
				MakeStructTypeFromFields("", FieldMap{"bar": StringType}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType}),
			},
			MakeUnionType(
				BoolType, StringType,
				MakeRefType(MakeUnionType(BoolType, StringType,
					MakeSetType(MakeUnionType(BoolType, StringType)))),
				MakeStructTypeFromFields("", FieldMap{"foo": NumberType, "bar": StringType}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType}),
			),
		},
	}

	for i, c := range cases {
		act := makeSimplifedType2(c.in...)
		assert.True(t, c.out.Equals(act), "Test case as position %d - got %s, expected %s", i, act.Describe(), c.out.Describe())
	}
}
