// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

var EmptyStructType = MakeStructType("", []string{}, []*Type{})
var EmptyStruct = Struct{ValueSlice{}, EmptyStructType, &hash.Hash{}}

type StructData map[string]Value

type Struct struct {
	values []Value
	t      *Type
	h      *hash.Hash
}

func NewStruct(name string, data StructData) Struct {
	fieldNames := make(sort.StringSlice, len(data))
	i := 0
	for fn := range data {
		fieldNames[i] = fn
		i++
	}

	sort.Sort(fieldNames)
	fieldTypes := make([]*Type, len(data))
	values := make(ValueSlice, len(data))
	for i, fn := range fieldNames {
		fieldTypes[i] = data[fn].Type()
		values[i] = data[fn]
	}

	return Struct{values, MakeStructType(name, fieldNames, fieldTypes), &hash.Hash{}}
}

func NewStructWithType(t *Type, data ValueSlice) Struct {
	desc := t.Desc.(StructDesc)
	d.PanicIfFalse(len(data) == len(desc.fields))
	for i, field := range desc.fields {
		v := data[i]
		assertSubtype(field.t, v)
	}
	return Struct{data, t, &hash.Hash{}}
}

func (s Struct) hashPointer() *hash.Hash {
	return s.h
}

// Value interface
func (s Struct) Equals(other Value) bool {
	return s.Hash() == other.Hash()
}

func (s Struct) Less(other Value) bool {
	return valueLess(s, other)
}

func (s Struct) Hash() hash.Hash {
	if s.h.IsEmpty() {
		*s.h = getHash(s)
	}

	return *s.h
}

func (s Struct) ChildValues() []Value {
	return s.values
}

func (s Struct) Chunks() (chunks []Ref) {
	chunks = append(chunks, s.t.Chunks()...)
	for _, v := range s.values {
		chunks = append(chunks, v.Chunks()...)
	}

	return
}

func (s Struct) Type() *Type {
	return s.t
}

func (s Struct) desc() StructDesc {
	return s.t.Desc.(StructDesc)
}

func (s Struct) MaybeGet(n string) (Value, bool) {
	_, i := s.desc().findField(n)
	if i == -1 {
		return nil, false
	}
	return s.values[i], true
}

func (s Struct) Get(n string) Value {
	_, i := s.desc().findField(n)
	if i == -1 {
		d.Chk.Fail(fmt.Sprintf(`Struct has no field "%s"`, n))
	}
	return s.values[i]
}

func (s Struct) Set(n string, v Value) Struct {
	f, i := s.desc().findField(n)
	if i == -1 {
		d.Chk.Fail(fmt.Sprintf(`Struct has no field "%s"`, n))
	}
	assertSubtype(f.t, v)
	values := make([]Value, len(s.values))
	copy(values, s.values)
	values[i] = v

	return Struct{values, s.t, &hash.Hash{}}
}

func (s Struct) Diff(last Struct, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	fs1, fs2 := s.Type().Desc.(StructDesc).fields, last.Type().Desc.(StructDesc).fields
	i1, i2 := 0, 0
	for i1 < len(fs1) && i2 < len(fs2) {
		f1, f2 := fs1[i1], fs2[i2]
		fn1, fn2 := f1.name, f2.name

		var change ValueChanged
		if fn1 == fn2 {
			if !s.values[i1].Equals(last.values[i2]) {
				change = ValueChanged{ChangeType: DiffChangeModified, V: String(fn1)}
			}
			i1++
			i2++
		} else if fn1 < fn2 {
			change = ValueChanged{ChangeType: DiffChangeAdded, V: String(fn1)}
			i1++
		} else {
			change = ValueChanged{ChangeType: DiffChangeRemoved, V: String(fn2)}
			i2++
		}

		if change != (ValueChanged{}) && !sendChange(changes, closeChan, change) {
			return
		}
	}

	for ; i1 < len(fs1); i1++ {
		if !sendChange(changes, closeChan, ValueChanged{ChangeType: DiffChangeAdded, V: String(fs1[i1].name)}) {
			return
		}
	}

	for ; i2 < len(fs2); i2++ {
		if !sendChange(changes, closeChan, ValueChanged{ChangeType: DiffChangeRemoved, V: String(fs2[i2].name)}) {
			return
		}
	}
}

var escapeChar = "Q"
var headFieldNamePattern = regexp.MustCompile("[a-zA-Z]")
var tailFieldNamePattern = regexp.MustCompile("[a-zA-Z0-9_]")
var spaceRegex = regexp.MustCompile("[ ]")
var escapeRegex = regexp.MustCompile(escapeChar)

var fieldNameComponentRe = regexp.MustCompile("^" + headFieldNamePattern.String() + tailFieldNamePattern.String() + "*")
var fieldNameRe = regexp.MustCompile(fieldNameComponentRe.String() + "$")

type encodingFunc func(string, *regexp.Regexp) string

func CamelCaseFieldName(input string) string {
	//strip invalid struct characters and leave spaces
	encode := func(s1 string, p *regexp.Regexp) string {
		if p.MatchString(s1) || spaceRegex.MatchString(s1) {
			return s1
		}
		return ""
	}

	strippedField := escapeField(input, encode)
	splitField := strings.Fields(strippedField)

	if len(splitField) == 0 {
		return ""
	}

	//Camelcase field
	output := strings.ToLower(splitField[0])
	if len(splitField) > 1 {
		for _, field := range splitField[1:] {
			output += strings.Title(strings.ToLower(field))
		}
	}
	//Because we are removing characters, we may generate an invalid field name
	//i.e. -- 1A B, we will remove the first bad chars and process until 1aB
	//1aB is invalid struct field name so we will return ""
	if !IsValidStructFieldName(output) {
		return ""
	}
	return output
}

func escapeField(input string, encode encodingFunc) string {
	output := ""
	pattern := headFieldNamePattern
	for _, ch := range input {
		output += encode(string([]rune{ch}), pattern)
		pattern = tailFieldNamePattern
	}
	return output
}

// EscapeStructField escapes names for use as noms structs with regards to non CSV imported data.
// Disallowed characters are encoded as 'Q<hex-encoded-utf8-bytes>'.
// Note that Q itself is also escaped since it is the escape character.
func EscapeStructField(input string) string {
	if !escapeRegex.MatchString(input) && IsValidStructFieldName(input) {
		return input
	}
	encode := func(s1 string, p *regexp.Regexp) string {
		if p.MatchString(s1) && s1 != escapeChar {
			return s1
		}

		var hs = fmt.Sprintf("%X", s1)
		var buf bytes.Buffer
		buf.WriteString(escapeChar)
		if len(hs) == 1 {
			buf.WriteString("0")
		}
		buf.WriteString(hs)
		return buf.String()
	}
	return escapeField(input, encode)
}

// IsValidStructFieldName returns whether the name is valid as a field name in a struct.
// Valid names must start with `a-zA-Z` and after that `a-zA-Z0-9_`.
func IsValidStructFieldName(name string) bool {
	return fieldNameRe.MatchString(name)
}

func verifyFieldNames(names []string) {
	if len(names) == 0 {
		return
	}

	last := names[0]
	verifyFieldName(last)

	for i := 1; i < len(names); i++ {
		verifyFieldName(names[i])
		if strings.Compare(names[i], last) <= 0 {
			d.Chk.Fail("Field names must be unique and ordered alphabetically")
		}
		last = names[i]
	}
}

func verifyName(name, kind string) {
	d.PanicIfTrue(!IsValidStructFieldName(name), `Invalid struct%s name: "%s"`, kind, name)
}

func verifyFieldName(name string) {
	verifyName(name, " field")
}

func verifyStructName(name string) {
	if name != "" {
		verifyName(name, "")
	}
}
