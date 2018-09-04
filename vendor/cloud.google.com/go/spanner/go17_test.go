// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build go1.7

package spanner

import (
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	proto "github.com/golang/protobuf/proto"
	proto3 "github.com/golang/protobuf/ptypes/struct"
	"golang.org/x/net/context"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

func TestEncodeStructValueDynamicStructs(t *testing.T) {
	dynStructType := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeOf(0), Tag: `spanner:"a"`},
		{Name: "B", Type: reflect.TypeOf(""), Tag: `spanner:"b"`},
	})
	dynNullableStructType := reflect.PtrTo(dynStructType)
	dynStructArrType := reflect.SliceOf(dynStructType)
	dynNullableStructArrType := reflect.SliceOf(dynNullableStructType)

	dynStructValue := reflect.New(dynStructType)
	dynStructValue.Elem().Field(0).SetInt(10)
	dynStructValue.Elem().Field(1).SetString("abc")

	dynStructArrValue := reflect.MakeSlice(dynNullableStructArrType, 2, 2)
	dynStructArrValue.Index(0).Set(reflect.Zero(dynNullableStructType))
	dynStructArrValue.Index(1).Set(dynStructValue)

	structProtoType := structType(
		mkField("a", intType()),
		mkField("b", stringType()))

	arrProtoType := listType(structProtoType)

	for _, test := range []encodeTest{
		{
			"Dynanic non-NULL struct value.",
			dynStructValue.Elem().Interface(),
			listProto(intProto(10), stringProto("abc")),
			structProtoType,
		},
		{
			"Dynanic NULL struct value.",
			reflect.Zero(dynNullableStructType).Interface(),
			nullProto(),
			structProtoType,
		},
		{
			"Empty array of dynamic structs.",
			reflect.MakeSlice(dynStructArrType, 0, 0).Interface(),
			listProto([]*proto3.Value{}...),
			arrProtoType,
		},
		{
			"NULL array of non-NULL-able dynamic structs.",
			reflect.Zero(dynStructArrType).Interface(),
			nullProto(),
			arrProtoType,
		},
		{
			"NULL array of NULL-able(nil) dynamic structs.",
			reflect.Zero(dynNullableStructArrType).Interface(),
			nullProto(),
			arrProtoType,
		},
		{
			"Array containing NULL(nil) dynamic-typed struct elements.",
			dynStructArrValue.Interface(),
			listProto(
				nullProto(),
				listProto(intProto(10), stringProto("abc"))),
			arrProtoType,
		},
	} {
		encodeStructValue(test, t)
	}
}

func TestEncodeStructValueEmptyStruct(t *testing.T) {
	emptyListValue := listProto([]*proto3.Value{}...)
	emptyStructType := structType([]*sppb.StructType_Field{}...)
	emptyStruct := struct{}{}
	nullEmptyStruct := (*struct{})(nil)

	dynamicEmptyStructType := reflect.StructOf(make([]reflect.StructField, 0, 0))
	dynamicStructArrType := reflect.SliceOf(reflect.PtrTo((dynamicEmptyStructType)))

	dynamicEmptyStruct := reflect.New(dynamicEmptyStructType)
	dynamicNullEmptyStruct := reflect.Zero(reflect.PtrTo(dynamicEmptyStructType))

	dynamicStructArrValue := reflect.MakeSlice(dynamicStructArrType, 2, 2)
	dynamicStructArrValue.Index(0).Set(dynamicNullEmptyStruct)
	dynamicStructArrValue.Index(1).Set(dynamicEmptyStruct)

	for _, test := range []encodeTest{
		{
			"Go empty struct.",
			emptyStruct,
			emptyListValue,
			emptyStructType,
		},
		{
			"Dynamic empty struct.",
			dynamicEmptyStruct.Interface(),
			emptyListValue,
			emptyStructType,
		},
		{
			"Go NULL empty struct.",
			nullEmptyStruct,
			nullProto(),
			emptyStructType,
		},
		{
			"Dynamic NULL empty struct.",
			dynamicNullEmptyStruct.Interface(),
			nullProto(),
			emptyStructType,
		},
		{
			"Non-empty array of dynamic NULL and non-NULL empty structs.",
			dynamicStructArrValue.Interface(),
			listProto(nullProto(), emptyListValue),
			listType(emptyStructType),
		},
		{
			"Non-empty array of nullable empty structs.",
			[]*struct{}{nullEmptyStruct, &emptyStruct},
			listProto(nullProto(), emptyListValue),
			listType(emptyStructType),
		},
		{
			"Empty array of empty struct.",
			[]struct{}{},
			emptyListValue,
			listType(emptyStructType),
		},
		{
			"Null array of empty structs.",
			[]struct{}(nil),
			nullProto(),
			listType(emptyStructType),
		},
	} {
		encodeStructValue(test, t)
	}
}

func TestEncodeStructValueMixedStructTypes(t *testing.T) {
	type staticStruct struct {
		F int `spanner:"fStatic"`
	}
	s1 := staticStruct{10}
	s2 := (*staticStruct)(nil)

	var f float64
	dynStructType := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeOf(f), Tag: `spanner:"fDynamic"`},
	})
	s3 := reflect.New(dynStructType)
	s3.Elem().Field(0).SetFloat(3.14)

	for _, test := range []encodeTest{
		{
			"'struct' with static and dynamic *struct, []*struct, []struct fields",
			struct {
				A []staticStruct
				B []*staticStruct
				C interface{}
			}{
				[]staticStruct{s1, s1},
				[]*staticStruct{&s1, s2},
				s3.Interface(),
			},
			listProto(
				listProto(listProto(intProto(10)), listProto(intProto(10))),
				listProto(listProto(intProto(10)), nullProto()),
				listProto(floatProto(3.14))),
			structType(
				mkField("A", listType(structType(mkField("fStatic", intType())))),
				mkField("B", listType(structType(mkField("fStatic", intType())))),
				mkField("C", structType(mkField("fDynamic", floatType())))),
		},
	} {
		encodeStructValue(test, t)
	}
}

func TestBindParamsDynamic(t *testing.T) {
	// Verify Statement.bindParams generates correct values and types.
	st := Statement{
		SQL:    "SELECT id from t_foo WHERE col = @var",
		Params: map[string]interface{}{"var": nil},
	}
	want := &sppb.ExecuteSqlRequest{
		Params: &proto3.Struct{
			Fields: map[string]*proto3.Value{"var": nil},
		},
		ParamTypes: map[string]*sppb.Type{"var": nil},
	}
	var (
		t1, _ = time.Parse(time.RFC3339Nano, "2016-11-15T15:04:05.999999999Z")
		// Boundaries
		t2, _ = time.Parse(time.RFC3339Nano, "0001-01-01T00:00:00.000000000Z")
	)
	dynamicStructType := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeOf(t1), Tag: `spanner:"field"`},
		{Name: "B", Type: reflect.TypeOf(3.14), Tag: `spanner:""`},
	})
	dynamicStructArrType := reflect.SliceOf(reflect.PtrTo(dynamicStructType))
	dynamicEmptyStructType := reflect.StructOf(make([]reflect.StructField, 0, 0))

	dynamicStructTypeProto := structType(
		mkField("field", timeType()),
		mkField("", floatType()))

	s3 := reflect.New(dynamicStructType)
	s3.Elem().Field(0).Set(reflect.ValueOf(t1))
	s3.Elem().Field(1).SetFloat(1.4)

	s4 := reflect.New(dynamicStructType)
	s4.Elem().Field(0).Set(reflect.ValueOf(t2))
	s4.Elem().Field(1).SetFloat(-13.3)

	dynamicStructArrayVal := reflect.MakeSlice(dynamicStructArrType, 2, 2)
	dynamicStructArrayVal.Index(0).Set(s3)
	dynamicStructArrayVal.Index(1).Set(s4)

	for i, test := range []struct {
		val       interface{}
		wantField *proto3.Value
		wantType  *sppb.Type
	}{
		{
			s3.Interface(),
			listProto(timeProto(t1), floatProto(1.4)),
			structType(
				mkField("field", timeType()),
				mkField("", floatType())),
		},
		{
			reflect.Zero(reflect.PtrTo(dynamicEmptyStructType)).Interface(),
			nullProto(),
			structType([]*sppb.StructType_Field{}...),
		},
		{
			dynamicStructArrayVal.Interface(),
			listProto(
				listProto(timeProto(t1), floatProto(1.4)),
				listProto(timeProto(t2), floatProto(-13.3))),
			listType(dynamicStructTypeProto),
		},
		{
			[]*struct {
				F1 time.Time `spanner:"field"`
				F2 float64   `spanner:""`
			}{
				nil,
				{t1, 1.4},
			},
			listProto(
				nullProto(),
				listProto(timeProto(t1), floatProto(1.4))),
			listType(dynamicStructTypeProto),
		},
	} {
		st.Params["var"] = test.val
		want.Params.Fields["var"] = test.wantField
		want.ParamTypes["var"] = test.wantType
		got := &sppb.ExecuteSqlRequest{}
		if err := st.bindParams(got); err != nil || !proto.Equal(got, want) {
			// handle NaN
			if test.wantType.Code == floatType().Code && proto.MarshalTextString(got) == proto.MarshalTextString(want) {
				continue
			}
			t.Errorf("#%d: bind result: \n(%v, %v)\nwant\n(%v, %v)\n", i, got, err, want, nil)
		}
	}
}

func TestStructParametersBind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, _, tearDown := prepare(ctx, t, nil)
	defer tearDown()

	type tRow []interface{}
	type tRows []struct{ trow tRow }

	type allFields struct {
		Stringf string
		Intf    int
		Boolf   bool
		Floatf  float64
		Bytef   []byte
		Timef   time.Time
		Datef   civil.Date
	}
	allColumns := []string{
		"Stringf",
		"Intf",
		"Boolf",
		"Floatf",
		"Bytef",
		"Timef",
		"Datef",
	}
	s1 := allFields{"abc", 300, false, 3.45, []byte("foo"), t1, d1}
	s2 := allFields{"def", -300, false, -3.45, []byte("bar"), t2, d2}

	dynamicStructType := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeOf(t1), Tag: `spanner:"ff1"`},
	})
	s3 := reflect.New(dynamicStructType)
	s3.Elem().Field(0).Set(reflect.ValueOf(t1))

	for i, test := range []struct {
		param interface{}
		sql   string
		cols  []string
		trows tRows
	}{
		// Struct value.
		{
			s1,
			"SELECT" +
				" @p.Stringf," +
				" @p.Intf," +
				" @p.Boolf," +
				" @p.Floatf," +
				" @p.Bytef," +
				" @p.Timef," +
				" @p.Datef",
			allColumns,
			tRows{
				{tRow{"abc", 300, false, 3.45, []byte("foo"), t1, d1}},
			},
		},
		// Array of struct value.
		{
			[]allFields{s1, s2},
			"SELECT * FROM UNNEST(@p)",
			allColumns,
			tRows{
				{tRow{"abc", 300, false, 3.45, []byte("foo"), t1, d1}},
				{tRow{"def", -300, false, -3.45, []byte("bar"), t2, d2}},
			},
		},
		// Null struct.
		{
			(*allFields)(nil),
			"SELECT @p IS NULL",
			[]string{""},
			tRows{
				{tRow{true}},
			},
		},
		// Null Array of struct.
		{
			[]allFields(nil),
			"SELECT @p IS NULL",
			[]string{""},
			tRows{
				{tRow{true}},
			},
		},
		// Empty struct.
		{
			struct{}{},
			"SELECT @p IS NULL ",
			[]string{""},
			tRows{
				{tRow{false}},
			},
		},
		// Empty array of struct.
		{
			[]allFields{},
			"SELECT * FROM UNNEST(@p) ",
			allColumns,
			tRows{},
		},
		// Struct with duplicate fields.
		{
			struct {
				A int `spanner:"field"`
				B int `spanner:"field"`
			}{10, 20},
			"SELECT * FROM UNNEST([@p]) ",
			[]string{"field", "field"},
			tRows{
				{tRow{10, 20}},
			},
		},
		// Struct with unnamed fields.
		{
			struct {
				A string `spanner:""`
			}{"hello"},
			"SELECT * FROM UNNEST([@p]) ",
			[]string{""},
			tRows{
				{tRow{"hello"}},
			},
		},
		// Mixed struct.
		{
			struct {
				DynamicStructField interface{}  `spanner:"f1"`
				ArrayStructField   []*allFields `spanner:"f2"`
			}{
				DynamicStructField: s3.Interface(),
				ArrayStructField:   []*allFields{nil},
			},
			"SELECT @p.f1.ff1, ARRAY_LENGTH(@p.f2), @p.f2[OFFSET(0)] IS NULL ",
			[]string{"ff1", "", ""},
			tRows{
				{tRow{t1, 1, true}},
			},
		},
	} {
		iter := client.Single().Query(ctx, Statement{
			SQL:    test.sql,
			Params: map[string]interface{}{"p": test.param},
		})
		var gotRows []*Row
		err := iter.Do(func(r *Row) error {
			gotRows = append(gotRows, r)
			return nil
		})
		if err != nil {
			t.Errorf("Failed to execute test case %d, error: %v", i, err)
		}

		var wantRows []*Row
		for j, row := range test.trows {
			r, err := NewRow(test.cols, row.trow)
			if err != nil {
				t.Errorf("Invalid row %d in test case %d", j, i)
			}
			wantRows = append(wantRows, r)
		}
		if !testEqual(gotRows, wantRows) {
			t.Errorf("%d: Want result %v, got result %v", i, wantRows, gotRows)
		}
	}
}
