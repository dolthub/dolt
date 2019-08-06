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

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainCommonSupertype(t *testing.T) {
	cases := []struct {
		a, b *Type
		out  bool
	}{
		// bool & any -> true
		{ValueType, StringType, true},
		// ref<bool> & ref<bool> -> true
		{mustType(MakeRefType(BoolType)), mustType(MakeRefType(BoolType)), true},
		// ref<number> & ref<string> -> false
		{mustType(MakeRefType(FloaTType)), mustType(MakeRefType(StringType)), false},
		// set<bool> & set<bool> -> true
		{mustType(MakeSetType(BoolType)), mustType(MakeSetType(BoolType)), true},
		// set<bool> & set<string> -> false
		{mustType(MakeSetType(BoolType)), mustType(MakeSetType(StringType)), false},
		// list<blob> & list<blob> -> true
		{mustType(MakeListType(BlobType)), mustType(MakeListType(BlobType)), true},
		// list<blob> & list<string> -> false
		{mustType(MakeListType(BlobType)), mustType(MakeListType(StringType)), false},
		// list<blob|string|number> & list<string|bool> -> true
		{mustType(MakeListType(mustType(MakeUnionType(BlobType, StringType, FloaTType)))), mustType(MakeListType(mustType(MakeUnionType(StringType, BoolType)))), true},
		// list<blob|string> & list<number|bool> -> false
		{mustType(MakeListType(mustType(MakeUnionType(BlobType, StringType)))), mustType(MakeListType(mustType(MakeUnionType(FloaTType, BoolType)))), false},

		// map<bool,bool> & map<bool,bool> -> true
		{mustType(MakeMapType(BoolType, BoolType)), mustType(MakeMapType(BoolType, BoolType)), true},
		// map<bool,bool> & map<bool,string> -> false
		{mustType(MakeMapType(BoolType, BoolType)), mustType(MakeMapType(BoolType, StringType)), false},
		// map<bool,bool> & map<string,bool> -> false
		{mustType(MakeMapType(BoolType, BoolType)), mustType(MakeMapType(StringType, BoolType)), false},
		// map<bool,bool> & map<string,bool> -> false
		{mustType(MakeMapType(BoolType, BoolType)), mustType(MakeMapType(StringType, BoolType)), false},
		// map<struct{foo:string},bool> & map<struct{foo:string,bar:string},bool> -> false
		{mustType(MakeMapType(mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})), BoolType)),
			mustType(MakeMapType(mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType, "bar": StringType})), BoolType)), false},
		// map<string|blob,string> & map<number|string,string> -> true
		{mustType(MakeMapType(mustType(MakeUnionType(StringType, BlobType)), StringType)),
			mustType(MakeMapType(mustType(MakeUnionType(FloaTType, StringType)), StringType)), true},
		// map<blob|bool,string> & map<number|string,string> -> false
		{mustType(MakeMapType(mustType(MakeUnionType(BlobType, BoolType)), StringType)),
			mustType(MakeMapType(mustType(MakeUnionType(FloaTType, StringType)), StringType)), false},

		// bool & string|bool|blob -> true
		{BoolType, mustType(MakeUnionType(StringType, BoolType, BlobType)), true},
		// string|bool|blob & blob -> true
		{mustType(MakeUnionType(StringType, BoolType, BlobType)), BlobType, true},
		// string|bool|blob & number|blob|string -> true
		{mustType(MakeUnionType(StringType, BoolType, BlobType)), mustType(MakeUnionType(FloaTType, BlobType, StringType)), true},

		// struct{foo:bool} & struct{foo:bool} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType})), true},
		// struct{foo:bool} & struct{foo:number} -> false
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})), false},
		// struct{foo:bool} & struct{foo:bool,bar:number} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType, "bar": FloaTType})), true},
		// struct{foo:ref<bool>} & struct{foo:ref<number>} -> false
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(BoolType))})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(FloaTType))})), false},
		// struct{foo:ref<bool>} & struct{foo:ref<number|bool>} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(BoolType))})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeRefType(mustType(MakeUnionType(FloaTType, BoolType))))})), true},
		// struct A{foo:bool} & struct A{foo:bool, baz:string} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType, "baz": StringType})), true},

		// struct A{foo:bool, stuff:set<String|Blob>} & struct A{foo:bool, stuff:set<String>} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType, "stuff": mustType(MakeSetType(mustType(MakeUnionType(StringType, BlobType))))})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType, "stuff": mustType(MakeSetType(StringType))})), true},
		// struct A{stuff:set<String|Blob>} & struct A{foo:bool, stuff:set<Float>} -> false
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType, "stuff": mustType(MakeSetType(mustType(MakeUnionType(StringType, BlobType))))})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"stuff": mustType(MakeSetType(FloaTType))})), false},

		// struct A{foo:bool} & struct {foo:bool} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType})), true},
		// struct {foo:bool} & struct A{foo:bool} -> false
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType})), true},
		// struct A{foo:bool} & struct B{foo:bool} -> false
		{mustType(MakeStructTypeFromFields("A", FieldMap{"foo": BoolType})),
			mustType(MakeStructTypeFromFields("B", FieldMap{"foo": BoolType})), false},
		// map<string, struct A{foo:string}> & map<string, struct A{foo:string, bar:bool}> -> true
		{mustType(MakeMapType(StringType, mustType(MakeStructTypeFromFields("A", FieldMap{"foo": StringType})))),
			mustType(MakeMapType(StringType, mustType(MakeStructTypeFromFields("A", FieldMap{"foo": StringType, "bar": BoolType})))), true},

		// struct{foo: string} & struct{foo: string|blob} -> true
		{mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeUnionType(StringType, BlobType))})), true},

		// struct{foo: string}|struct{foo: blob} & struct{foo: string|blob} -> true
		{mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": BlobType}))),
		), mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeUnionType(StringType, BlobType))})), true},
		// struct{foo: string}|struct{foo: blob} & struct{foo: number|bool} -> false
		{mustType(MakeUnionType(
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": StringType})),
			mustType(MakeStructTypeFromFields("", FieldMap{"foo": BlobType}))),
		), mustType(MakeStructTypeFromFields("", FieldMap{"foo": mustType(MakeUnionType(FloaTType, BoolType))})), false},

		// map<struct{x:number, y:number}, struct A{foo:string}> & map<struct{x:number, y:number}, struct A{foo:string, bar:bool}> -> true
		{
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": FloaTType, "y": FloaTType})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": StringType})))),
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": FloaTType, "y": FloaTType})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": StringType, "bar": BoolType})))),
			true,
		},

		// map<struct{x:number, y:number}, struct A{foo:string}> & map<struct{x:number, y:number}, struct A{foo:string, bar:bool}> -> true
		{
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": FloaTType, "y": FloaTType})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": StringType})))),
			mustType(MakeMapType(
				mustType(MakeStructTypeFromFields("", FieldMap{"x": FloaTType, "y": FloaTType})),
				mustType(MakeStructTypeFromFields("A", FieldMap{"foo": StringType, "bar": BoolType})))),
			true,
		},

		// struct A{self:A} & struct A{self:A, foo:Float} -> true
		{mustType(MakeStructTypeFromFields("A", FieldMap{"self": MakeCycleType("A")})),
			mustType(MakeStructTypeFromFields("A", FieldMap{"self": MakeCycleType("A"), "foo": FloaTType})), true},

		// struct{b:Bool} & struct{b?:Bool} -> true
		{
			mustType(MakeStructType("", StructField{"b", BoolType, false})),
			mustType(MakeStructType("", StructField{"b", BoolType, true})),
			true,
		},

		// struct{a?:Bool} & struct{b?:Bool} -> false
		{
			mustType(MakeStructType("", StructField{"a", BoolType, true})),
			mustType(MakeStructType("", StructField{"b", BoolType, true})),
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

	for i, c := range cases {
		act := ContainCommonSupertype(Format_7_18, c.a, c.b)
		aDesc, err := c.a.Describe(context.Background())
		assert.NoError(t, err)
		bDesc, err := c.b.Describe(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, c.out, act, "Test case at position %d; \n\ta:%s\n\tb:%s", i, aDesc, bDesc)
	}
}
