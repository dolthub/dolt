// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getChunks(v Value) (chunks []Ref) {
	v.WalkRefs(Format_7_18, func(r Ref) {
		chunks = append(chunks, r)
	})
	return
}

func TestGenericStructEquals(t *testing.T) {
	assert := assert.New(t)

	s1 := NewStruct(Format_7_18, "S1", StructData{"s": String("hi"), "x": Bool(true)})
	s2 := NewStruct(Format_7_18, "S1", StructData{"s": String("hi"), "x": Bool(true)})

	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
}

func TestGenericStructChunks(t *testing.T) {
	assert := assert.New(t)

	b := Bool(true)
	s1 := NewStruct(Format_7_18, "S1", StructData{"r": NewRef(b, Format_7_18)})

	assert.Len(getChunks(s1), 1)
	assert.Equal(Bool(true).Hash(Format_7_18), getChunks(s1)[0].TargetHash())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	s := NewStruct(Format_7_18, "S2", StructData{"b": Bool(true), "o": String("hi")})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("missing")
	assert.False(ok)

	s2 := NewStruct(Format_7_18, "S2", StructData{"b": Bool(false), "o": String("hi")})
	assert.True(s2.Get("b").Equals(Bool(false)))
	o, ok := s2.MaybeGet("o")
	assert.True(ok)
	assert.True(String("hi").Equals(o))
}

func TestGenericStructSet(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s := NewStruct(Format_7_18, "S3", StructData{"b": Bool(true), "o": String("hi")})
	s2 := s.Set("b", Bool(false))

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))

	// Changes the type
	s4 := s.Set("b", Float(42))
	assert.True(MakeStructType("S3",
		StructField{"b", FloaTType, false},
		StructField{"o", StringType, false},
	).Equals(TypeOf(s4)))

	// Adds a new field
	s5 := s.Set("x", Float(42))
	assert.True(MakeStructType("S3",
		StructField{"b", BoolType, false},
		StructField{"o", StringType, false},
		StructField{"x", FloaTType, false},
	).Equals(TypeOf(s5)))

	// Subtype is not equal.
	// TODO(binformat)
	s6 := NewStruct(Format_7_18, "", StructData{"l": NewList(context.Background(), vs, Float(0), Float(1), Bool(false), Bool(true))})
	s7 := s6.Set("l", NewList(context.Background(), vs, Float(2), Float(3)))
	t7 := MakeStructTypeFromFields("", FieldMap{
		"l": MakeListType(FloaTType),
	})
	assert.True(t7.Equals(TypeOf(s7)))

	s8 := NewStruct(Format_7_18, "S", StructData{"a": Bool(true), "c": Bool(true)})
	s9 := s8.Set("b", Bool(true))
	assert.True(s9.Equals(NewStruct(Format_7_18, "S", StructData{"a": Bool(true), "b": Bool(true), "c": Bool(true)})))
}

func TestGenericStructDelete(t *testing.T) {
	assert := assert.New(t)

	s1 := NewStruct(Format_7_18, "S", StructData{"b": Bool(true), "o": String("hi")})

	s2 := s1.Delete("notThere")
	assert.True(s1.Equals(s2))

	s3 := s1.Delete("o")
	s4 := NewStruct(Format_7_18, "S", StructData{"b": Bool(true)})
	assert.True(s3.Equals(s4))

	s5 := s3.Delete("b")
	s6 := NewStruct(Format_7_18, "S", StructData{})
	assert.True(s5.Equals(s6))
}

func assertValueChangeEqual(assert *assert.Assertions, c1, c2 ValueChanged) {
	assert.Equal(c1.ChangeType, c2.ChangeType)
	assert.Equal(EncodedValue(context.Background(), c1.Key), EncodedValue(context.Background(), c2.Key))
	if c1.NewValue == nil {
		assert.Nil(c2.NewValue)
	} else {
		assert.Equal(EncodedValue(context.Background(), c1.NewValue), EncodedValue(context.Background(), c2.NewValue))
	}
	if c1.OldValue == nil {
		assert.Nil(c2.OldValue)
	} else {
		assert.Equal(EncodedValue(context.Background(), c1.OldValue), EncodedValue(context.Background(), c2.OldValue))
	}
}

func TestStructDiff(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	assertDiff := func(expect []ValueChanged, s1, s2 Struct) {
		changes := make(chan ValueChanged)
		go func() {
			s1.Diff(s2, changes, nil)
			close(changes)
		}()
		i := 0
		for change := range changes {
			assertValueChangeEqual(assert, expect[i], change)
			i++
		}
		assert.Equal(len(expect), i, "Wrong number of changes")
	}

	vc := func(ct DiffChangeType, fieldName string, oldV, newV Value) ValueChanged {
		return ValueChanged{ct, String(fieldName), oldV, newV}
	}

	s1 := NewStruct(Format_7_18, "", StructData{"a": Bool(true), "b": String("hi"), "c": Float(4)})

	assertDiff([]ValueChanged{},
		s1, NewStruct(Format_7_18, "", StructData{"a": Bool(true), "b": String("hi"), "c": Float(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeModified, "a", Bool(false), Bool(true)), vc(DiffChangeModified, "b", String("bye"), String("hi"))},
		s1, NewStruct(Format_7_18, "", StructData{"a": Bool(false), "b": String("bye"), "c": Float(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeModified, "b", String("bye"), String("hi")), vc(DiffChangeModified, "c", Float(5), Float(4))},
		s1, NewStruct(Format_7_18, "", StructData{"a": Bool(true), "b": String("bye"), "c": Float(5)}))

	assertDiff([]ValueChanged{vc(DiffChangeModified, "a", Bool(false), Bool(true)), vc(DiffChangeModified, "c", Float(10), Float(4))},
		s1, NewStruct(Format_7_18, "", StructData{"a": Bool(false), "b": String("hi"), "c": Float(10)}))

	assertDiff([]ValueChanged{vc(DiffChangeAdded, "a", nil, Bool(true))},
		s1, NewStruct(Format_7_18, "NewType", StructData{"b": String("hi"), "c": Float(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeAdded, "b", nil, String("hi"))},
		s1, NewStruct(Format_7_18, "NewType", StructData{"a": Bool(true), "c": Float(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeRemoved, "Z", Float(17), nil)},
		s1, NewStruct(Format_7_18, "NewType", StructData{"Z": Float(17), "a": Bool(true), "b": String("hi"), "c": Float(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeAdded, "b", nil, String("hi")), vc(DiffChangeRemoved, "d", Float(5), nil)},
		s1, NewStruct(Format_7_18, "NewType", StructData{"a": Bool(true), "c": Float(4), "d": Float(5)}))

	s2 := NewStruct(Format_7_18, "", StructData{
		// TODO(binformat)
		"a": NewList(context.Background(), vs, Float(0), Float(1)),
		"b": NewMap(context.Background(), vs, String("foo"), Bool(false), String("bar"), Bool(true)),
		"c": NewSet(context.Background(), vs, Float(0), Float(1), String("foo")),
	})

	assertDiff([]ValueChanged{},
		s2, NewStruct(Format_7_18, "", StructData{
			// TODO(binformat)
			"a": NewList(context.Background(), vs, Float(0), Float(1)),
			"b": NewMap(context.Background(), vs, String("foo"), Bool(false), String("bar"), Bool(true)),
			"c": NewSet(context.Background(), vs, Float(0), Float(1), String("foo")),
		}))

	assertDiff([]ValueChanged{
		// TODO(binformat)
		vc(DiffChangeModified, "a",
			NewList(context.Background(), vs, Float(1), Float(1)),
			NewList(context.Background(), vs, Float(0), Float(1))),
		vc(DiffChangeModified, "b",
			NewMap(context.Background(), vs, String("foo"), Bool(true), String("bar"), Bool(true)),
			NewMap(context.Background(), vs, String("foo"), Bool(false), String("bar"), Bool(true))),
	},
		s2, NewStruct(Format_7_18, "", StructData{
			// TODO(binformat)
			"a": NewList(context.Background(), vs, Float(1), Float(1)),
			"b": NewMap(context.Background(), vs, String("foo"), Bool(true), String("bar"), Bool(true)),
			"c": NewSet(context.Background(), vs, Float(0), Float(1), String("foo")),
		}))

	assertDiff([]ValueChanged{
		// TODO(binformat)
		vc(DiffChangeModified, "a", NewList(context.Background(), vs, Float(0)), NewList(context.Background(), vs, Float(0), Float(1))),
		vc(DiffChangeModified, "c", NewSet(context.Background(), vs, Float(0), Float(2), String("foo")), NewSet(context.Background(), vs, Float(0), Float(1), String("foo"))),
	},
		s2, NewStruct(Format_7_18, "", StructData{
			// TODO(binformat)
			"a": NewList(context.Background(), vs, Float(0)),
			"b": NewMap(context.Background(), vs, String("foo"), Bool(false), String("bar"), Bool(true)),
			"c": NewSet(context.Background(), vs, Float(0), Float(2), String("foo")),
		}))

	assertDiff([]ValueChanged{
		vc(DiffChangeModified, "b", NewMap(context.Background(), vs, String("boo"), Bool(false), String("bar"), Bool(true)), NewMap(context.Background(), vs, String("foo"), Bool(false), String("bar"), Bool(true))),
		vc(DiffChangeModified, "c", NewSet(context.Background(), vs, Float(0), Float(1), String("bar")), NewSet(context.Background(), vs, Float(0), Float(1), String("foo"))),
	},
		s2, NewStruct(Format_7_18, "", StructData{
			// TODO(binformat)
			"a": NewList(context.Background(), vs, Float(0), Float(1)),
			"b": NewMap(context.Background(), vs, String("boo"), Bool(false), String("bar"), Bool(true)),
			"c": NewSet(context.Background(), vs, Float(0), Float(1), String("bar")),
		}))
}

func TestEscStructField(t *testing.T) {
	assert := assert.New(t)
	cases := []string{
		"a", "a",
		"AaZz19_", "AaZz19_",
		"Q", "Q51",
		"AQ1", "AQ511",
		"INSPECTIONQ20STATUS", "INSPECTIONQ5120STATUS",
		"$", "Q24",
		"_content", "Q5Fcontent",
		"Few ¢ents Short", "FewQ20QC2A2entsQ20Short",
		"💩", "QF09F92A9",
		"https://picasaweb.google.com/data", "httpsQ3AQ2FQ2FpicasawebQ2EgoogleQ2EcomQ2Fdata",
	}

	for i := 0; i < len(cases); i += 2 {
		orig, expected := cases[i], cases[i+1]
		assert.Equal(expected, EscapeStructField(orig))
	}
}

func TestMakeStructTemplate(t *testing.T) {
	assert := assert.New(t)

	assertInvalidStructName := func(n string) {
		assert.Panics(func() {
			MakeStructTemplate(n, []string{})
		})
	}

	assertInvalidStructName(" ")
	assertInvalidStructName(" a")
	assertInvalidStructName("a ")
	assertInvalidStructName("0")
	assertInvalidStructName("_")
	assertInvalidStructName("0a")
	assertInvalidStructName("_a")
	assertInvalidStructName("💩")

	assertValidStructName := func(n string) {
		template := MakeStructTemplate(n, []string{})
		str := template.NewStruct(Format_7_18, nil)
		assert.Equal(n, str.Name())
	}

	assertValidStructName("")
	assertValidStructName("a")
	assertValidStructName("A")
	assertValidStructName("a0")
	assertValidStructName("a_")
	assertValidStructName("a0_")

	assertInvalidFieldName := func(n string) {
		assert.Panics(func() {
			MakeStructTemplate("", []string{n})
		})
	}

	assertInvalidFieldName("")
	assertInvalidFieldName(" ")
	assertInvalidFieldName(" a")
	assertInvalidFieldName("a ")
	assertInvalidFieldName("0")
	assertInvalidFieldName("_")
	assertInvalidFieldName("0a")
	assertInvalidFieldName("_a")
	assertInvalidFieldName("💩")

	assertValidFieldName := func(n string) {
		MakeStructTemplate("", []string{n})
	}

	assertValidFieldName("a")
	assertValidFieldName("A")
	assertValidFieldName("a0")
	assertValidFieldName("a_")
	assertValidFieldName("a0_")

	assertInvalidFieldOrder := func(n []string) {
		assert.Panics(func() {
			MakeStructTemplate("", n)
		})
	}

	assertInvalidFieldOrder([]string{"a", "a"})
	assertInvalidFieldOrder([]string{"b", "a"})
	assertInvalidFieldOrder([]string{"a", "c", "b"})

	assertValidFieldOrder := func(n []string) {
		MakeStructTemplate("", n)
	}

	assertValidFieldOrder([]string{"a", "b"})
	assertValidFieldOrder([]string{"a", "b", "c"})

	template := MakeStructTemplate("A", []string{"a", "b"})
	str := template.NewStruct(Format_7_18, []Value{Float(42), Bool(true)})
	assert.True(NewStruct(Format_7_18, "A", StructData{
		"a": Float(42),
		"b": Bool(true),
	}).Equals(str))
}

func TestStructWithNil(t *testing.T) {
	assert.Panics(t, func() {
		NewStruct(Format_7_18, "A", StructData{
			"a": nil,
		})
	})
	assert.Panics(t, func() {
		NewStruct(Format_7_18, "A", StructData{
			"a": Float(42),
			"b": nil,
		})
	})
}
