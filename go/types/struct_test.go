// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func getChunks(v Value) (chunks []Ref) {
	v.WalkRefs(func(r Ref) {
		chunks = append(chunks, r)
	})
	return
}

func TestGenericStructEquals(t *testing.T) {
	assert := assert.New(t)

	s1 := NewStruct("S1", StructData{"s": String("hi"), "x": Bool(true)})
	s2 := NewStruct("S1", StructData{"s": String("hi"), "x": Bool(true)})

	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
}

func TestGenericStructChunks(t *testing.T) {
	assert := assert.New(t)

	b := Bool(true)
	s1 := NewStruct("S1", StructData{"r": NewRef(b)})

	assert.Len(getChunks(s1), 1)
	assert.Equal(Bool(true).Hash(), getChunks(s1)[0].TargetHash())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	s := NewStruct("S2", StructData{"b": Bool(true), "o": String("hi")})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("missing")
	assert.False(ok)

	s2 := NewStruct("S2", StructData{"b": Bool(false), "o": String("hi")})
	assert.True(s2.Get("b").Equals(Bool(false)))
	o, ok := s2.MaybeGet("o")
	assert.True(ok)
	assert.True(String("hi").Equals(o))
}

func TestGenericStructSet(t *testing.T) {
	assert := assert.New(t)

	s := NewStruct("S3", StructData{"b": Bool(true), "o": String("hi")})
	s2 := s.Set("b", Bool(false))

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))

	// Changes the type
	s4 := s.Set("b", Number(42))
	assert.True(MakeStructType("S3",
		StructField{"b", NumberType, false},
		StructField{"o", StringType, false},
	).Equals(TypeOf(s4)))

	// Adds a new field
	s5 := s.Set("x", Number(42))
	assert.True(MakeStructType("S3",
		StructField{"b", BoolType, false},
		StructField{"o", StringType, false},
		StructField{"x", NumberType, false},
	).Equals(TypeOf(s5)))

	// Subtype is not equal.
	s6 := NewStruct("", StructData{"l": NewList(Number(0), Number(1), Bool(false), Bool(true))})
	s7 := s6.Set("l", NewList(Number(2), Number(3)))
	t7 := MakeStructTypeFromFields("", FieldMap{
		"l": MakeListType(NumberType),
	})
	assert.True(t7.Equals(TypeOf(s7)))
}

func TestGenericStructDelete(t *testing.T) {
	assert := assert.New(t)

	s1 := NewStruct("S", StructData{"b": Bool(true), "o": String("hi")})

	s2 := s1.Delete("notThere")
	assert.True(s1.Equals(s2))

	s3 := s1.Delete("o")
	s4 := NewStruct("S", StructData{"b": Bool(true)})
	assert.True(s3.Equals(s4))

	s5 := s3.Delete("b")
	s6 := NewStruct("S", StructData{})
	assert.True(s5.Equals(s6))
}

func assertValueChangeEqual(assert *assert.Assertions, c1, c2 ValueChanged) {
	assert.Equal(c1.ChangeType, c2.ChangeType)
	assert.Equal(EncodedValue(c1.Key), EncodedValue(c2.Key))
	if c1.NewValue == nil {
		assert.Nil(c2.NewValue)
	} else {
		assert.Equal(EncodedValue(c1.NewValue), EncodedValue(c2.NewValue))
	}
	if c1.OldValue == nil {
		assert.Nil(c2.OldValue)
	} else {
		assert.Equal(EncodedValue(c1.OldValue), EncodedValue(c2.OldValue))
	}
}

func TestStructDiff(t *testing.T) {
	assert := assert.New(t)

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

	s1 := NewStruct("", StructData{"a": Bool(true), "b": String("hi"), "c": Number(4)})

	assertDiff([]ValueChanged{},
		s1, NewStruct("", StructData{"a": Bool(true), "b": String("hi"), "c": Number(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeModified, "a", Bool(false), Bool(true)), vc(DiffChangeModified, "b", String("bye"), String("hi"))},
		s1, NewStruct("", StructData{"a": Bool(false), "b": String("bye"), "c": Number(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeModified, "b", String("bye"), String("hi")), vc(DiffChangeModified, "c", Number(5), Number(4))},
		s1, NewStruct("", StructData{"a": Bool(true), "b": String("bye"), "c": Number(5)}))

	assertDiff([]ValueChanged{vc(DiffChangeModified, "a", Bool(false), Bool(true)), vc(DiffChangeModified, "c", Number(10), Number(4))},
		s1, NewStruct("", StructData{"a": Bool(false), "b": String("hi"), "c": Number(10)}))

	assertDiff([]ValueChanged{vc(DiffChangeAdded, "a", nil, Bool(true))},
		s1, NewStruct("NewType", StructData{"b": String("hi"), "c": Number(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeAdded, "b", nil, String("hi"))},
		s1, NewStruct("NewType", StructData{"a": Bool(true), "c": Number(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeRemoved, "Z", Number(17), nil)},
		s1, NewStruct("NewType", StructData{"Z": Number(17), "a": Bool(true), "b": String("hi"), "c": Number(4)}))

	assertDiff([]ValueChanged{vc(DiffChangeAdded, "b", nil, String("hi")), vc(DiffChangeRemoved, "d", Number(5), nil)},
		s1, NewStruct("NewType", StructData{"a": Bool(true), "c": Number(4), "d": Number(5)}))

	s2 := NewStruct("", StructData{
		"a": NewList(Number(0), Number(1)),
		"b": NewMap(String("foo"), Bool(false), String("bar"), Bool(true)),
		"c": NewSet(Number(0), Number(1), String("foo")),
	})

	assertDiff([]ValueChanged{},
		s2, NewStruct("", StructData{
			"a": NewList(Number(0), Number(1)),
			"b": NewMap(String("foo"), Bool(false), String("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(1), String("foo")),
		}))

	assertDiff([]ValueChanged{
		vc(DiffChangeModified, "a", NewList(Number(1), Number(1)), NewList(Number(0), Number(1))),
		vc(DiffChangeModified, "b", NewMap(String("foo"), Bool(true), String("bar"), Bool(true)), NewMap(String("foo"), Bool(false), String("bar"), Bool(true))),
	},
		s2, NewStruct("", StructData{
			"a": NewList(Number(1), Number(1)),
			"b": NewMap(String("foo"), Bool(true), String("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(1), String("foo")),
		}))

	assertDiff([]ValueChanged{
		vc(DiffChangeModified, "a", NewList(Number(0)), NewList(Number(0), Number(1))),
		vc(DiffChangeModified, "c", NewSet(Number(0), Number(2), String("foo")), NewSet(Number(0), Number(1), String("foo"))),
	},
		s2, NewStruct("", StructData{
			"a": NewList(Number(0)),
			"b": NewMap(String("foo"), Bool(false), String("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(2), String("foo")),
		}))

	assertDiff([]ValueChanged{
		vc(DiffChangeModified, "b", NewMap(String("boo"), Bool(false), String("bar"), Bool(true)), NewMap(String("foo"), Bool(false), String("bar"), Bool(true))),
		vc(DiffChangeModified, "c", NewSet(Number(0), Number(1), String("bar")), NewSet(Number(0), Number(1), String("foo"))),
	},
		s2, NewStruct("", StructData{
			"a": NewList(Number(0), Number(1)),
			"b": NewMap(String("boo"), Bool(false), String("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(1), String("bar")),
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
		str := template.NewStruct(nil)
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
	str := template.NewStruct([]Value{Number(42), Bool(true)})
	assert.True(NewStruct("A", StructData{
		"a": Number(42),
		"b": Bool(true),
	}).Equals(str))
}

func TestStructWithNil(t *testing.T) {
	assert.Panics(t, func() {
		NewStruct("A", StructData{
			"a": nil,
		})
	})
	assert.Panics(t, func() {
		NewStruct("A", StructData{
			"a": Number(42),
			"b": nil,
		})
	})
}
