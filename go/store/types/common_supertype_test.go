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

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainCommonSupertype(t *testing.T) {
	cases := []struct {
		a, b *Type
		out  bool
	}{
		// bool & any -> true
		{PrimitiveTypeMap[ValueKind], PrimitiveTypeMap[StringKind], true},
		// ref<bool> & ref<bool> -> true
		{mustType(MakeRefType(PrimitiveTypeMap[BoolKind])), mustType(MakeRefType(PrimitiveTypeMap[BoolKind])), true},
		// ref<number> & ref<string> -> false
		{mustType(MakeRefType(PrimitiveTypeMap[FloatKind])), mustType(MakeRefType(PrimitiveTypeMap[StringKind])), false},
		// set<bool> & set<bool> -> true
		{mustType(MakeSetType(PrimitiveTypeMap[BoolKind])), mustType(MakeSetType(PrimitiveTypeMap[BoolKind])), true},
		// set<bool> & set<string> -> false
		{mustType(MakeSetType(PrimitiveTypeMap[BoolKind])), mustType(MakeSetType(PrimitiveTypeMap[StringKind])), false},
		// list<blob> & list<blob> -> true
		{mustType(MakeListType(PrimitiveTypeMap[BlobKind])), mustType(MakeListType(PrimitiveTypeMap[BlobKind])), true},
		// list<blob> & list<string> -> false
		{mustType(MakeListType(PrimitiveTypeMap[BlobKind])), mustType(MakeListType(PrimitiveTypeMap[StringKind])), false},
		// list<blob|string|number> & list<string|bool> -> true
		{mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[BlobKind], PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])))), mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])))), true},
		// list<blob|string> & list<number|bool> -> false
		{mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[BlobKind], PrimitiveTypeMap[StringKind])))), mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind])))), false},

		// map<bool,bool> & map<bool,bool> -> true
		{mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])), mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])), true},
		// map<bool,bool> & map<bool,string> -> false
		{mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])), mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])), false},
		// map<bool,bool> & map<string,bool> -> false
		{mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])), mustType(MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])), false},
		// map<bool,bool> & map<string,bool> -> false
		{mustType(MakeMapType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BoolKind])), mustType(MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind])), false},
		// map<struct{foo:string},bool> & map<struct{foo:string,bar:string},bool> -> false
		{mustType(MakeMapType(mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})), PrimitiveTypeMap[BoolKind])),
			mustType(MakeMapType(mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind], "bar": PrimitiveTypeMap[StringKind]})), PrimitiveTypeMap[BoolKind])), false},
		// map<string|blob,string> & map<number|string,string> -> true
		{mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BlobKind])), PrimitiveTypeMap[StringKind])),
			mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), PrimitiveTypeMap[StringKind])), true},
		// map<blob|bool,string> & map<number|string,string> -> false
		{mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[BlobKind], PrimitiveTypeMap[BoolKind])), PrimitiveTypeMap[StringKind])),
			mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), PrimitiveTypeMap[StringKind])), false},

		// bool & string|bool|blob -> true
		{PrimitiveTypeMap[BoolKind], mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BlobKind])), true},
		// string|bool|blob & blob -> true
		{mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BlobKind])), PrimitiveTypeMap[BlobKind], true},
		// string|bool|blob & number|blob|string -> true
		{mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[BlobKind])), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BlobKind], PrimitiveTypeMap[StringKind])), true},

		// struct{foo:bool} & struct{foo:bool} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})), true},
		// struct{foo:bool} & struct{foo:number} -> false
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})), false},
		// struct{foo:bool} & struct{foo:bool,bar:number} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind], "bar": PrimitiveTypeMap[FloatKind]})), true},
		// struct{foo:ref<bool>} & struct{foo:ref<number>} -> false
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(PrimitiveTypeMap[BoolKind]))})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(PrimitiveTypeMap[FloatKind]))})), false},
		// struct{foo:ref<bool>} & struct{foo:ref<number|bool>} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(PrimitiveTypeMap[BoolKind]))})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind]))))})), true},
		// struct A{foo:bool} & struct A{foo:bool, baz:string} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind], "baz": PrimitiveTypeMap[StringKind]})), true},

		// struct A{foo:bool, stuff:set<String|Blob>} & struct A{foo:bool, stuff:set<String>} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind], "stuff": mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BlobKind]))))})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind], "stuff": mustType(MakeSetType(PrimitiveTypeMap[StringKind]))})), true},
		// struct A{stuff:set<String|Blob>} & struct A{foo:bool, stuff:set<Float>} -> false
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind], "stuff": mustType(MakeSetType(mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BlobKind]))))})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"stuff": mustType(MakeSetType(PrimitiveTypeMap[FloatKind]))})), false},

		// struct A{foo:bool} & struct {foo:bool} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})), true},
		// struct {foo:bool} & struct A{foo:bool} -> false
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})), true},
		// struct A{foo:bool} & struct B{foo:bool} -> false
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})),
			mustType(MakeStructTypeFromFields("B", FieldMap{"foo": PrimitiveTypeMap[BoolKind]})), false},
		// map<string, struct A{foo:string}> & map<string, struct A{foo:string, bar:bool}> -> true
		{mustType(MakeMapType(PrimitiveTypeMap[StringKind], mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[StringKind]})))),
			mustType(MakeMapType(PrimitiveTypeMap[StringKind], mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[StringKind], "bar": PrimitiveTypeMap[BoolKind]})))), true},

		// struct{foo: string} & struct{foo: string|blob} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BlobKind]))})), true},

		// struct{foo: string}|struct{foo: blob} & struct{foo: string|blob} -> true
		{mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BlobKind]}))),
		), mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeUnionType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[BlobKind]))})), true},
		// struct{foo: string}|struct{foo: blob} & struct{foo: number|bool} -> false
		{mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[StringKind]})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": PrimitiveTypeMap[BlobKind]}))),
		), mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[BoolKind]))})), false},

		// map<struct{x:number, y:number}, struct A{foo:string}> & map<struct{x:number, y:number}, struct A{foo:string, bar:bool}> -> true
		{
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": PrimitiveTypeMap[FloatKind], "y": PrimitiveTypeMap[FloatKind]})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[StringKind]})))),
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": PrimitiveTypeMap[FloatKind], "y": PrimitiveTypeMap[FloatKind]})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[StringKind], "bar": PrimitiveTypeMap[BoolKind]})))),
			true,
		},

		// map<struct{x:number, y:number}, struct A{foo:string}> & map<struct{x:number, y:number}, struct A{foo:string, bar:bool}> -> true
		{
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": PrimitiveTypeMap[FloatKind], "y": PrimitiveTypeMap[FloatKind]})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[StringKind]})))),
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": PrimitiveTypeMap[FloatKind], "y": PrimitiveTypeMap[FloatKind]})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": PrimitiveTypeMap[StringKind], "bar": PrimitiveTypeMap[BoolKind]})))),
			true,
		},

		// struct A{self:A} & struct A{self:A, foo:Float} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"self": MakeCycleType("A")})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"self": MakeCycleType("A"), "foo": PrimitiveTypeMap[FloatKind]})), true},

		// struct{b:Bool} & struct{b?:Bool} -> true
		{
			mustType(MakeStructType("", StructField{"b", PrimitiveTypeMap[BoolKind], false})),
			mustType(MakeStructType("", StructField{"b", PrimitiveTypeMap[BoolKind], true})),
			true,
		},

		// struct{a?:Bool} & struct{b?:Bool} -> false
		{
			mustType(MakeStructType("", StructField{"a", PrimitiveTypeMap[BoolKind], true})),
			mustType(MakeStructType("", StructField{"b", PrimitiveTypeMap[BoolKind], true})),
			false,
		},

		// struct A {b: struct {a: Cycle<A>}} & struct {b: Struct A {b: struct {b: Cycle<A>}}} -> false
		{
			mustType(MakeStructType("A",
				StructField{"a", mustType(MakeStructType("",
					StructField{"a", MakeCycleType("A"), false},
				)), false},
			)),
			mustType(MakeStructType("",
				StructField{"a", mustType(MakeStructType("A",
					StructField{"a", mustType(MakeStructType("",
						StructField{"a", MakeCycleType("A"), false},
					)), false},
				)), false},
			)),
			true,
		},
	}

	vrw := newTestValueStore()

	for i, c := range cases {
		act := ContainCommonSupertype(vrw.Format(), c.a, c.b)
		aDesc, err := c.a.Describe(context.Background())
		require.NoError(t, err)
		bDesc, err := c.b.Describe(context.Background())
		require.NoError(t, err)
		assert.Equal(t, c.out, act, "Test case at position %d; \n\ta:%s\n\tb:%s", i, aDesc, bDesc)
	}
}
