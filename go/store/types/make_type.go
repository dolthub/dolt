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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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

	"github.com/dolthub/dolt/go/store/d"
)

func MakePrimitiveType(k NomsKind) (*Type, error) {
	if typ, ok := PrimitiveTypeMap[k]; ok {
		return typ, nil
	}
	return nil, ErrUnknownType
}

// MakeUnionType creates a new union type unless the elemTypes can be folded into a single non union type.
func MakeUnionType(elemTypes ...*Type) (*Type, error) {
	t, err := makeUnionType(elemTypes...)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

func MakeListType(elemType *Type) (*Type, error) {
	t, err := makeCompoundType(ListKind, elemType)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

func MakeSetType(elemType *Type) (*Type, error) {
	t, err := makeCompoundType(SetKind, elemType)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

func MakeRefType(elemType *Type) (*Type, error) {
	t, err := makeCompoundType(RefKind, elemType)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

func MakeMapType(keyType, valType *Type) (*Type, error) {
	t, err := makeCompoundType(MapKind, keyType, valType)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

func MakeStructType(name string, fields ...StructField) (*Type, error) {
	fs := structTypeFields(fields)
	sort.Sort(fs)
	t, err := makeStructType(name, fs)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

func MakeCycleType(name string) *Type {
	d.PanicIfTrue(name == "")
	return newType(CycleDesc(name))
}

func makePrimitiveType(k NomsKind) *Type {
	return newType(PrimitiveDesc(k))
}

// PrimitiveTypeMap auto populates with Value types that return true from isPrimitive().
// Only include a type here manually if it has no associated Value type.
var PrimitiveTypeMap = map[NomsKind]*Type{
	ValueKind: makePrimitiveType(ValueKind),
}

func makeCompoundType(kind NomsKind, elemTypes ...*Type) (*Type, error) {
	for _, el := range elemTypes {
		if el.Kind() == UnknownKind {
			// If any of the element's types are an unknown type then this is unknown
			return nil, ErrUnknownType
		}
	}

	return newType(CompoundDesc{elemTypes, kind}), nil
}

func makeUnionType(elemTypes ...*Type) (*Type, error) {
	if len(elemTypes) == 1 {
		return elemTypes[0], nil
	}
	return makeCompoundType(UnionKind, elemTypes...)
}

func makeStructTypeQuickly(name string, fields structTypeFields) (*Type, error) {
	for _, fld := range fields {
		if fld.Type.Kind() == UnknownKind {
			// If any of the fields have an unknown type then this is unknown
			return nil, ErrUnknownType
		}
	}

	return newType(StructDesc{name, fields}), nil
}

func makeStructType(name string, fields structTypeFields) (*Type, error) {
	verifyStructName(name)
	verifyFields(fields)
	return makeStructTypeQuickly(name, fields)
}

type FieldMap map[string]*Type

func MakeStructTypeFromFields(name string, fields FieldMap) (*Type, error) {
	fs := make(structTypeFields, len(fields))
	i := 0
	for k, v := range fields {
		fs[i] = StructField{v, k, false}
		i++
	}
	sort.Sort(fs)
	t, err := makeStructType(name, fs)

	if err != nil {
		return nil, err
	}

	return simplifyType(t, false)
}

// StructField describes a field in a struct type.
type StructField struct {
	Type     *Type
	Name     string
	Optional bool
}

type structTypeFields []StructField

func (s structTypeFields) Len() int           { return len(s) }
func (s structTypeFields) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s structTypeFields) Less(i, j int) bool { return s[i].Name < s[j].Name }

func verifyFields(fs structTypeFields) {
	for i, f := range fs {
		verifyFieldName(f.Name)
		if i > 0 && strings.Compare(fs[i-1].Name, f.Name) >= 0 {
			d.Panic("Field names must be unique and ordered alphabetically")
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

// IsValidStructFieldName returns whether the name is valid as a field name in a struct.
// Valid names must start with `a-zA-Z` and after that `a-zA-Z0-9_`.
func IsValidStructFieldName(name string) bool {
	for i, c := range name {
		if i == 0 {
			if !isAlpha(c) {
				return false
			}
		} else if !isAlphaNumOrUnderscore(c) {
			return false
		}
	}
	return len(name) != 0
}

func isAlpha(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isAlphaNumOrUnderscore(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

var escapeChar = "Q"
var headFieldNamePattern = regexp.MustCompile("[a-zA-Z]")
var tailFieldNamePattern = regexp.MustCompile("[a-zA-Z0-9_]")
var escapeRegex = regexp.MustCompile(escapeChar)

var fieldNameComponentRe = regexp.MustCompile("^" + headFieldNamePattern.String() + tailFieldNamePattern.String() + "*")

type encodingFunc func(string, *regexp.Regexp) string

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
