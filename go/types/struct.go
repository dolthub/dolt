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

var EmptyStructType = MakeStructType("")
var EmptyStruct = Struct{"", []string{}, []Value{}, &hash.Hash{}}

type StructData map[string]Value

type Struct struct {
	name       string
	fieldNames []string
	values     []Value
	h          *hash.Hash
}

func validateStruct(s Struct) Struct {
	verifyStructName(s.name)

	d.PanicIfFalse(len(s.fieldNames) == len(s.values))

	if len(s.fieldNames) == 0 {
		return s
	}

	verifyFieldName(s.fieldNames[0])
	d.PanicIfTrue(s.values[0] == nil)

	for i := 1; i < len(s.fieldNames); i++ {
		verifyFieldName(s.fieldNames[i])
		d.PanicIfFalse(s.fieldNames[i] > s.fieldNames[i-1])
		d.PanicIfTrue(s.values[i] == nil)
	}
	return s
}

func newStruct(name string, fieldNames []string, values []Value) Struct {
	return Struct{name, fieldNames, values, &hash.Hash{}}
}

func NewStruct(name string, data StructData) Struct {
	fieldNames := make([]string, len(data))
	values := make([]Value, len(data))

	i := 0
	for name, _ := range data {
		fieldNames[i] = name
		i++
	}

	sort.Sort(sort.StringSlice(fieldNames))
	for i = 0; i < len(fieldNames); i++ {
		values[i] = data[fieldNames[i]]
	}

	return validateStruct(newStruct(name, fieldNames, values))
}

// StructTemplate allows creating a template for structs with a known shape
// (name and fields). If a lot of structs of the same shape are being created
// then using a StructTemplate makes that slightly more efficient.
type StructTemplate struct {
	name       string
	fieldNames []string
}

// MakeStructTemplate creates a new StructTemplate or panics if the name and
// fields are not valid.
func MakeStructTemplate(name string, fieldNames []string) (t StructTemplate) {
	t = StructTemplate{name, fieldNames}

	verifyStructName(name)
	if len(fieldNames) == 0 {
		return
	}
	verifyFieldName(fieldNames[0])
	for i := 1; i < len(fieldNames); i++ {
		verifyFieldName(fieldNames[i])
		d.PanicIfFalse(fieldNames[i] > fieldNames[i-1])
	}
	return
}

// NewStruct creates a new Struct from the StructTemplate. The order of the
// values must match the order of the field names of the StructTemplate.
func (st StructTemplate) NewStruct(values []Value) Struct {
	d.PanicIfFalse(len(st.fieldNames) == len(values))
	return newStruct(st.name, st.fieldNames, values)
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

func (s Struct) WalkValues(cb ValueCallback) {
	for _, v := range s.values {
		cb(v)
	}
}

func (s Struct) WalkRefs(cb RefCallback) {
	for _, v := range s.values {
		v.WalkRefs(cb)
	}
}

func (s Struct) typeOf() *Type {
	typeFields := make(structTypeFields, len(s.fieldNames))
	for i := 0; i < len(s.fieldNames); i++ {
		typeFields[i] = StructField{
			Name:     s.fieldNames[i],
			Optional: false,
			Type:     s.values[i].typeOf(),
		}
	}
	return makeStructTypeQuickly(s.name, typeFields)
}

// Len is the number of fields in the struct.
func (s Struct) Len() int {
	return len(s.fieldNames)
}

// Name is the name of the struct.
func (s Struct) Name() string {
	return s.name
}

// IterFields iterates over the fields, calling cb for every field in the
// struct.
func (s Struct) IterFields(cb func(name string, value Value)) {
	for i := 0; i < len(s.fieldNames); i++ {
		cb(s.fieldNames[i], s.values[i])
	}
}

func (s Struct) Kind() NomsKind {
	return StructKind
}

// MaybeGet returns the value of a field in the struct. If the struct does not a have a field with
// the name name then this returns (nil, false).
func (s Struct) MaybeGet(n string) (Value, bool) {
	i := s.findField(n)
	if i == -1 {
		return nil, false
	}

	return s.values[i], true
}

func (s Struct) searchField(name string) int {
	return sort.Search(len(s.fieldNames), func(i int) bool { return s.fieldNames[i] >= name })
}

func (s Struct) findField(name string) int {
	i := s.searchField(name)
	if i == len(s.fieldNames) || s.fieldNames[i] != name {
		return -1
	}
	return i
}

// Get returns the value of a field in the struct. If the struct does not a have a field with the
// name name then this panics.
func (s Struct) Get(n string) Value {
	i := s.findField(n)
	if i == -1 {
		d.Chk.Fail(fmt.Sprintf(`Struct has no field "%s"`, n))
	}
	return s.values[i]
}

// Set returns a new struct where the field name has been set to value. If name is not an
// existing field in the struct or the type of value is different from the old value of the
// struct field a new struct type is created.
func (s Struct) Set(n string, v Value) Struct {
	i := s.searchField(n)

	if i != len(s.fieldNames) && s.fieldNames[i] == n {
		// Found
		values := make([]Value, len(s.fieldNames))
		copy(values, s.values)
		values[i] = v

		// No need to validate
		return newStruct(s.name, s.fieldNames, values)
	}

	fieldNames := make([]string, len(s.fieldNames)+1)
	copy(fieldNames[:i], s.fieldNames[:i])
	fieldNames[i] = n
	copy(fieldNames[i+1:], s.fieldNames[i:])

	values := make([]Value, len(s.fieldNames)+1)
	copy(values[:i], s.values[:i])
	values[i] = v
	copy(values[i+1:], s.values[i:])

	return validateStruct(newStruct(s.name, fieldNames, values))
}

// IsZeroValue can be used to test if a struct is the same as Struct{}.
func (s Struct) IsZeroValue() bool {
	return s.fieldNames == nil && s.values == nil && s.name == "" && s.h == nil
}

// Delete returns a new struct where the field name has been removed.
// If name is not an existing field in the struct then the current struct is returned.
func (s Struct) Delete(n string) Struct {
	i := s.findField(n)
	if i == -1 {
		return s
	}

	fieldNames := make([]string, len(s.fieldNames)-1)
	copy(fieldNames[:i], s.fieldNames[:i])
	copy(fieldNames[i:], s.fieldNames[i+1:])

	values := make([]Value, len(s.fieldNames)-1)
	copy(values[:i], s.values[:i])
	copy(values[i:], s.values[i+1:])

	// No need to validate
	return newStruct(s.name, fieldNames, values)
}

func (s Struct) Diff(last Struct, changes chan<- ValueChanged, closeChan <-chan struct{}) {
	if s.Equals(last) {
		return
	}
	fn1, fn2 := s.fieldNames, last.fieldNames
	i1, i2 := 0, 0
	for i1 < len(fn1) && i2 < len(fn2) {
		fn1, fn2 := fn1[i1], fn2[i2]

		var change ValueChanged
		if fn1 == fn2 {
			if !s.values[i1].Equals(last.values[i2]) {
				change = ValueChanged{DiffChangeModified, String(fn1), last.values[i2], s.values[i1]}
			}
			i1++
			i2++
		} else if fn1 < fn2 {
			change = ValueChanged{DiffChangeAdded, String(fn1), nil, s.values[i1]}
			i1++
		} else {
			change = ValueChanged{DiffChangeRemoved, String(fn2), last.values[i2], nil}
			i2++
		}

		if change != (ValueChanged{}) && !sendChange(changes, closeChan, change) {
			return
		}
	}

	for ; i1 < len(fn1); i1++ {
		if !sendChange(changes, closeChan, ValueChanged{DiffChangeAdded, String(fn1[i1]), nil, s.values[i1]}) {
			return
		}
	}

	for ; i2 < len(fn2); i2++ {
		if !sendChange(changes, closeChan, ValueChanged{DiffChangeRemoved, String(fn2[i2]), last.values[i2], nil}) {
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

func verifyFields(fs structTypeFields) {
	for i, f := range fs {
		verifyFieldName(f.Name)
		if i > 0 && strings.Compare(fs[i-1].Name, f.Name) >= 0 {
			d.Chk.Fail("Field names must be unique and ordered alphabetically")
		}
	}
}

func verifyName(name, kind string) {
	if !IsValidStructFieldName(name) {
		d.Panic(`Invalid struct%s name: "%s"`, kind, name)
	}
}

func verifyFieldName(name string) {
	verifyName(name, " field")
}

func verifyStructName(name string) {
	if name != "" {
		verifyName(name, "")
	}
}
