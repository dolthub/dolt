// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type structData map[string]Value

type Struct struct {
	values []Value
	t      *Type
	h      *hash.Hash
}

func NewStruct(name string, data structData) Struct {
	fieldNames := make(sort.StringSlice, len(data))
	i := 0
	for fn, _ := range data {
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
	d.Chk.True(len(data) == len(desc.fields))
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
	return other != nil && s.Hash() == other.Hash()
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
		d.Chk.Fail(`Struct has no field "%s"`, n)
	}
	return s.values[i]
}

func (s Struct) Set(n string, v Value) Struct {
	f, i := s.desc().findField(n)
	if i == -1 {
		d.Chk.Fail(`Struct has no field "%s"`, n)
	}
	assertSubtype(f.t, v)
	values := make([]Value, len(s.values))
	copy(values, s.values)
	values[i] = v

	return Struct{values, s.t, &hash.Hash{}}
}

// s1 & s2 must be of the same type. Returns the set of field names which have different values in the respective structs
func StructDiff(s1, s2 Struct) (changed []string) {
	d.Chk.True(s1.Type().Equals(s2.Type()))

	fields := s1.desc().fields
	for i, v1 := range s1.values {
		v2 := s2.values[i]
		if !v1.Equals(v2) {
			changed = append(changed, fields[i].name)
		}
	}

	return
}

var escapeChar = "Q"
var headPattern = regexp.MustCompile("[a-zA-PR-Z]")
var tailPattern = regexp.MustCompile("[a-zA-PR-Z1-9_]")
var completePattern = regexp.MustCompile("^" + headPattern.String() + tailPattern.String() + "*$")

// Escapes names for use as noms structs. Disallowed characters are encoded as
// 'Q<hex-encoded-utf8-bytes>'. Note that Q itself is also escaped since it is
// the escape character.
func EscapeStructField(input string) string {
	if completePattern.MatchString(input) {
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

	output := ""
	pattern := headPattern
	for _, ch := range input {
		output += encode(string([]rune{ch}), pattern)
		pattern = tailPattern
	}

	return output
}
