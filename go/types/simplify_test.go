package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

// testing strategy
// - test simplifying each kind in isolation, both shallow and deep
// - test makeSupertype
//   - pass one type only
//   - test that instances are properly deduplicated
//   - test union flattening
//   - test grouping of the various kinds
//   - test cycles

func TestSimplifyHelpers(t *testing.T) {
	structSimplifier := func(n string) func(typeset) *Type {
		return func(ts typeset) *Type {
			return simplifyStructs(n, ts)
		}
	}

	cases := []struct {
		f   func(typeset) *Type
		in  []*Type
		out *Type
	}{
		// Ref<Bool> -> Ref<Bool>
		{simplifyRefs,
			[]*Type{MakeRefType(BoolType)},
			MakeRefType(BoolType)},
		// Ref<Number>|Ref<String>|Ref<blob> -> Ref<Number|String|blob>
		{simplifyRefs,
			[]*Type{MakeRefType(NumberType), MakeRefType(StringType), MakeRefType(BlobType)},
			MakeRefType(MakeUnionType(NumberType, StringType, BlobType))},
		// Ref<set<Bool>>|Ref<set<String>> -> Ref<set<Bool|String>>
		{simplifyRefs,
			[]*Type{MakeRefType(MakeSetType(BoolType)), MakeRefType(MakeSetType(StringType))},
			MakeRefType(MakeSetType(MakeUnionType(BoolType, StringType)))},
		// Ref<set<Bool>|Ref<set<String>>|Ref<Number> -> Ref<set<Bool|String>|Number>
		{simplifyRefs,
			[]*Type{MakeRefType(MakeSetType(BoolType)), MakeRefType(MakeSetType(StringType)), MakeRefType(NumberType)},
			MakeRefType(MakeUnionType(MakeSetType(MakeUnionType(BoolType, StringType)), NumberType))},

		// set<Bool> -> set<Bool>
		{simplifySets,
			[]*Type{MakeSetType(BoolType)},
			MakeSetType(BoolType)},
		// set<Number>|set<String>|set<blob> -> set<Number|String|blob>
		{simplifySets,
			[]*Type{MakeSetType(NumberType), MakeSetType(StringType), MakeSetType(BlobType)},
			MakeSetType(MakeUnionType(NumberType, StringType, BlobType))},
		// set<set<Bool>>|set<set<String>> -> set<set<Bool|String>>
		{simplifySets,
			[]*Type{MakeSetType(MakeSetType(BoolType)), MakeSetType(MakeSetType(StringType))},
			MakeSetType(MakeSetType(MakeUnionType(BoolType, StringType)))},
		// set<set<Bool>|set<set<String>>|set<Number> -> set<set<Bool|String>|Number>
		{simplifySets,
			[]*Type{MakeSetType(MakeSetType(BoolType)), MakeSetType(MakeSetType(StringType)), MakeSetType(NumberType)},
			MakeSetType(MakeUnionType(MakeSetType(MakeUnionType(BoolType, StringType)), NumberType))},

		// list<Bool> -> list<Bool>
		{simplifyLists,
			[]*Type{MakeListType(BoolType)},
			MakeListType(BoolType)},
		// list<Number>|list<String>|list<blob> -> list<Number|String|blob>
		{simplifyLists,
			[]*Type{MakeListType(NumberType), MakeListType(StringType), MakeListType(BlobType)},
			MakeListType(MakeUnionType(NumberType, StringType, BlobType))},
		// list<set<Bool>>|list<set<String>> -> list<set<Bool|String>>
		{simplifyLists,
			[]*Type{MakeListType(MakeListType(BoolType)), MakeListType(MakeListType(StringType))},
			MakeListType(MakeListType(MakeUnionType(BoolType, StringType)))},
		// list<set<Bool>|list<set<String>>|list<Number> -> list<set<Bool|String>|Number>
		{simplifyLists,
			[]*Type{MakeListType(MakeListType(BoolType)), MakeListType(MakeListType(StringType)), MakeListType(NumberType)},
			MakeListType(MakeUnionType(MakeListType(MakeUnionType(BoolType, StringType)), NumberType))},

		// map<Bool,bool> -> map<Bool,bool>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType)},
			MakeMapType(BoolType, BoolType)},
		// map<Bool,bool>|map<Bool,string> -> map<Bool,bool|String>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType), MakeMapType(BoolType, StringType)},
			MakeMapType(BoolType, MakeUnionType(BoolType, StringType))},
		// map<Bool,bool>|map<String,bool> -> map<Bool|String,bool>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType), MakeMapType(StringType, BoolType)},
			MakeMapType(MakeUnionType(BoolType, StringType), BoolType)},
		// map<Bool,bool>|map<String,string> -> map<Bool|String,bool|String>
		{simplifyMaps,
			[]*Type{MakeMapType(BoolType, BoolType), MakeMapType(StringType, StringType)},
			MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, StringType))},
		// map<set<Bool>,bool>|map<set<String>,string> -> map<set<Bool|String>,bool|String>
		{simplifyMaps,
			[]*Type{MakeMapType(MakeSetType(BoolType), BoolType), MakeMapType(MakeSetType(StringType), StringType)},
			MakeMapType(MakeSetType(MakeUnionType(BoolType, StringType)), MakeUnionType(BoolType, StringType))},

		// struct{foo:Bool} -> struct{foo:Bool}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
			MakeStructTypeFromFields("", FieldMap{"foo": BoolType})},
		// struct{foo:Bool}|struct{foo:Number} -> struct{foo:Bool|Number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeUnionType(BoolType, StringType)})},
		// struct{foo:Bool}|struct{foo:Bool,bar:Number} -> struct{foo:Bool,bar?:Number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"foo": BoolType, "bar": NumberType})},
			MakeStructType2("",
				StructField{"bar", NumberType, true},
				StructField{"foo", BoolType, false},
			)},
		// struct{foo:Bool}|struct{bar:Number} -> struct{foo?:Bool,bar?:Number}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("", FieldMap{"bar": NumberType})},
			MakeStructType2("",
				StructField{"bar", NumberType, true},
				StructField{"foo", BoolType, true},
			)},
		// struct{foo:Ref<Bool>}|struct{foo:Ref<Number>} -> struct{foo:Ref<Bool|Number>}
		{structSimplifier(""),
			[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(BoolType)}),
				MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(NumberType)})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeRefType(MakeUnionType(BoolType, NumberType))})},

		// struct A{foo:Bool}|struct A{foo:String} -> struct A{foo:Bool|String}
		{structSimplifier("A"),
			[]*Type{MakeStructTypeFromFields("A", FieldMap{"foo": BoolType}),
				MakeStructTypeFromFields("A", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("A", FieldMap{"foo": MakeUnionType(BoolType, StringType)})},

		// map<String, struct A{foo:String}>,  map<String, struct A{foo:String, bar:Bool}>
		// 	-> map<String, struct A{foo:String,bar?:Bool}>
		{simplifyMaps,
			[]*Type{MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType})),
				MakeMapType(StringType, MakeStructTypeFromFields("A", FieldMap{"foo": StringType, "bar": BoolType})),
			},
			MakeMapType(StringType, MakeStructType2("A",
				StructField{"foo", StringType, false},
				StructField{"bar", BoolType, true},
			))},
	}

	for i, c := range cases {
		act := c.f(newTypeset(c.in...))
		assert.True(t, c.out.Equals(act), "Test case as position %d - got %s, wanted %s", i, act.Describe(), c.out.Describe())
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
		// {bool,Number} -> bool|Number
		{[]*Type{BoolType, NumberType},
			MakeUnionType(BoolType, NumberType)},
		// {bool,Number|(string|blob|Number)} -> bool|Number|String|blob
		{[]*Type{BoolType, MakeUnionType(NumberType, MakeUnionType(StringType, BlobType, NumberType))},
			MakeUnionType(BoolType, NumberType, StringType, BlobType)},

		// {Ref<Number>} -> Ref<Number>
		{[]*Type{MakeRefType(NumberType)},
			MakeRefType(NumberType)},
		// {Ref<Number>,Ref<String>} -> Ref<Number|String>
		{[]*Type{MakeRefType(NumberType), MakeRefType(StringType)},
			MakeRefType(MakeUnionType(NumberType, StringType))},

		// {set<Number>} -> set<Number>
		{[]*Type{MakeSetType(NumberType)},
			MakeSetType(NumberType)},
		// {set<Number>,set<String>} -> set<Number|String>
		{[]*Type{MakeSetType(NumberType), MakeSetType(StringType)},
			MakeSetType(MakeUnionType(NumberType, StringType))},

		// {list<Number>} -> list<Number>
		{[]*Type{MakeListType(NumberType)},
			MakeListType(NumberType)},
		// {list<Number>,list<String>} -> list<Number|String>
		{[]*Type{MakeListType(NumberType), MakeListType(StringType)},
			MakeListType(MakeUnionType(NumberType, StringType))},

		// {map<Number,Number>} -> map<Number,Number>
		{[]*Type{MakeMapType(NumberType, NumberType)},
			MakeMapType(NumberType, NumberType)},
		// {map<Number,Number>,map<String,string>} -> map<Number|String,Number|String>
		{[]*Type{MakeMapType(NumberType, NumberType), MakeMapType(StringType, StringType)},
			MakeMapType(MakeUnionType(NumberType, StringType), MakeUnionType(NumberType, StringType))},

		// {struct{foo:Number}} -> struct{foo:Number}
		{[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": NumberType})},
			MakeStructTypeFromFields("", FieldMap{"foo": NumberType})},
		// {struct{foo:Number}, struct{foo:String}} -> struct{foo:Number|String}
		{[]*Type{MakeStructTypeFromFields("", FieldMap{"foo": NumberType}),
			MakeStructTypeFromFields("", FieldMap{"foo": StringType})},
			MakeStructTypeFromFields("", FieldMap{"foo": MakeUnionType(NumberType, StringType)})},

		// {Bool,String,Ref<Bool>,Ref<String>,Ref<Set<String>>,Ref<Set<Bool>>,
		//    struct{foo:Number},struct{bar:String},struct A{foo:Number}} ->
		// Bool|String|Ref<Bool|String|Set<String|Bool>>|struct{foo?:Number,bar?:String}|struct A{foo:Number}
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
				MakeStructType2("",
					StructField{"foo", NumberType, true},
					StructField{"bar", StringType, true},
				),
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
