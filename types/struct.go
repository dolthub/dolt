// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/hash"
)

type structData map[string]Value

type Struct struct {
	data structData
	t    *Type
	h    *hash.Hash
}

func newStructFromData(data structData, t *Type) Struct {
	d.Chk.Equal(t.Kind(), StructKind)
	return Struct{data, t, &hash.Hash{}}
}

func NewStruct(name string, data structData) Struct {
	fields := make(TypeMap, len(data))
	newData := make(structData, len(data))
	for k, v := range data {
		fields[k] = v.Type()
		newData[k] = v
	}
	t := MakeStructType(name, fields)
	return newStructFromData(newData, t)
}

func NewStructWithType(t *Type, data structData) Struct {
	newData := make(structData, len(data))
	desc := t.Desc.(StructDesc)
	for name, t := range desc.Fields {
		v, ok := data[name]
		d.Chk.True(ok, "Missing required field %s", name)
		assertSubtype(t, v)
		newData[name] = v
	}
	return newStructFromData(newData, t)
}

func (s Struct) hashPointer() *hash.Hash {
	return s.h
}

// Value interface
func (s Struct) Equals(other Value) bool {
	return other != nil && s.t.Equals(other.Type()) && s.Hash() == other.Hash()
}

func (s Struct) Less(other Value) bool {
	return valueLess(s, other)
}

func (s Struct) Hash() hash.Hash {
	return EnsureHash(s.h, s)
}

func (s Struct) ChildValues() (res []Value) {
	s.desc().IterFields(func(name string, t *Type) {
		v, ok := s.data[name]
		d.Chk.True(ok)
		res = append(res, v)
	})
	return
}

func (s Struct) Chunks() (chunks []Ref) {
	chunks = append(chunks, s.t.Chunks()...)
	for name := range s.desc().Fields {
		v, ok := s.data[name]
		d.Chk.True(ok)
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
	_, ok := s.findField(n)
	if !ok {
		return nil, false
	}
	v, ok := s.data[n]
	return v, ok
}

func (s Struct) Get(n string) Value {
	_, ok := s.findField(n)
	d.Chk.True(ok, `Struct has no field "%s"`, n)
	v, ok := s.data[n]
	d.Chk.True(ok)
	return v
}

func (s Struct) Set(n string, v Value) Struct {
	t, ok := s.findField(n)
	d.Chk.True(ok, "Struct has no field %s", n)
	assertSubtype(t, v)
	data := make(structData, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	data[n] = v

	return newStructFromData(data, s.t)
}

func (s Struct) findField(n string) (*Type, bool) {
	for name, typ := range s.desc().Fields {
		if name == n {
			return typ, true
		}
	}
	return nil, false
}

func structBuilder(values []Value, t *Type) Value {
	desc := t.Desc.(StructDesc)
	data := structData{}

	i := 0
	desc.IterFields(func(name string, t *Type) {
		data[name] = values[i]
		i++
	})

	return newStructFromData(data, t)
}

func structReader(s Struct, t *Type) []Value {
	d.Chk.Equal(t.Kind(), StructKind)
	values := []Value{}

	desc := t.Desc.(StructDesc)
	desc.IterFields(func(name string, t *Type) {
		v, ok := s.data[name]
		d.Chk.True(ok)
		values = append(values, v)
	})

	return values
}

// s1 & s2 must be of the same type. Returns the set of field names which have different values in the respective structs
func StructDiff(s1, s2 Struct) (changed []string) {
	d.Chk.True(s1.Type().Equals(s2.Type()))

	s1.desc().IterFields(func(name string, t *Type) {
		v1 := s1.data[name]
		v2 := s2.data[name]
		if !v1.Equals(v2) {
			changed = append(changed, name)
		}
	})

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
