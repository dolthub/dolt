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
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/store/d"
)

var EmptyStructType, _ = MakeStructType("")

func EmptyStruct(nbf *NomsBinFormat) Struct {
	es, _ := newStruct(nbf, "", nil, nil)
	return es
}

type StructData map[string]Value

type Struct struct {
	valueImpl
}

// readStruct reads the data provided by a decoder and moves the decoder forward.
func readStruct(nbf *NomsBinFormat, dec *valueDecoder) (Struct, error) {
	start := dec.pos()
	err := skipStruct(nbf, dec)

	if err != nil {
		return EmptyStruct(nbf), err
	}

	end := dec.pos()
	return Struct{valueImpl{dec.vrw, nbf, dec.byteSlice(start, end), nil}}, nil
}

func skipStruct(nbf *NomsBinFormat, dec *valueDecoder) error {
	dec.skipKind()
	dec.skipString() // name
	count := dec.readCount()
	for i := uint64(0); i < count; i++ {
		dec.skipString()
		err := dec.SkipValue(nbf)

		if err != nil {
			return err
		}
	}

	return nil
}

func isStructSameTypeForSure(nbf *NomsBinFormat, dec *valueDecoder, t *Type) (bool, error) {
	desc := t.Desc.(StructDesc)
	dec.skipKind()
	if !dec.isStringSame(desc.Name) {
		return false, nil
	}
	count := dec.readCount()
	if count != uint64(len(desc.fields)) {
		return false, nil
	}
	for i := uint64(0); i < count; i++ {
		if desc.fields[i].Optional {
			return false, nil
		}
		if !dec.isStringSame(desc.fields[i].Name) {
			return false, nil
		}

		isSame, err := dec.isValueSameTypeForSure(nbf, desc.fields[i].Type)

		if err != nil {
			return false, err
		}

		if !isSame {
			return false, nil
		}
	}
	return true, nil
}

func walkStruct(nbf *NomsBinFormat, r *refWalker, cb RefCallback) error {
	r.skipKind()
	r.skipString() // name
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.skipString()
		err := r.walkValue(nbf, cb)

		if err != nil {
			return err
		}
	}

	return nil
}

func newStruct(nbf *NomsBinFormat, name string, fieldNames []string, values []Value) (Struct, error) {
	var vrw ValueReadWriter
	w := newBinaryNomsWriter()
	err := StructKind.writeTo(&w, nbf)

	if err != nil {
		return EmptyStruct(nbf), err
	}

	w.writeString(name)
	w.writeCount(uint64(len(fieldNames)))
	for i := 0; i < len(fieldNames); i++ {
		w.writeString(fieldNames[i])
		if vrw == nil {
			vrw = values[i].(valueReadWriter).valueReadWriter()
		}
		err := values[i].writeTo(&w, nbf)

		if err != nil {
			return EmptyStruct(nbf), err
		}
	}
	return Struct{valueImpl{vrw, nbf, w.data(), nil}}, nil
}

func NewStruct(nbf *NomsBinFormat, name string, data StructData) (Struct, error) {
	verifyStructName(name)
	fieldNames := make([]string, len(data))
	values := make([]Value, len(data))

	i := 0
	for name := range data {
		verifyFieldName(name)
		fieldNames[i] = name
		i++
	}

	sort.Strings(fieldNames)
	for i = 0; i < len(fieldNames); i++ {
		values[i] = data[fieldNames[i]]
	}

	return newStruct(nbf, name, fieldNames, values)
}

func (s Struct) Format() *NomsBinFormat {
	return s.format()
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
func (st StructTemplate) NewStruct(nbf *NomsBinFormat, values []Value) (Struct, error) {
	d.PanicIfFalse(len(st.fieldNames) == len(values))
	return newStruct(nbf, st.name, st.fieldNames, values)
}

func (s Struct) Empty() bool {
	return s.Len() == 0
}

// Value interface
func (s Struct) isPrimitive() bool {
	return false
}

func (s Struct) Value(ctx context.Context) (Value, error) {
	return s, nil
}

func (s Struct) typeOf() (*Type, error) {
	dec := s.decoder()
	return readStructTypeOfValue(s.format(), &dec)
}

func readStructTypeOfValue(nbf *NomsBinFormat, dec *valueDecoder) (*Type, error) {
	dec.skipKind()
	name := dec.ReadString()
	count := dec.readCount()
	typeFields := make(structTypeFields, count)
	for i := uint64(0); i < count; i++ {
		fname := dec.ReadString()
		t, err := dec.readTypeOfValue(nbf)

		if err != nil {
			return nil, fmt.Errorf("error decoding type of field %s: %w", fname, err)
		}

		typeFields[i] = StructField{
			Name:     fname,
			Optional: false,
			Type:     t,
		}
	}

	return makeStructTypeQuickly(name, typeFields)
}

func (s Struct) decoderSkipToFields() (valueDecoder, uint64) {
	dec := s.decoder()
	dec.skipKind()
	dec.skipString()
	count := dec.readCount()
	return dec, count
}

// Len is the number of fields in the struct.
func (s Struct) Len() int {
	_, count := s.decoderSkipToFields()
	return int(count)
}

// Name is the name of the struct.
func (s Struct) Name() string {
	dec := s.decoder()
	dec.skipKind()
	return dec.ReadString()
}

// IterFields iterates over the fields, calling cb for every field in the
// struct.
func (s Struct) IterFields(cb func(name string, value Value) error) error {
	dec, count := s.decoderSkipToFields()
	for i := uint64(0); i < count; i++ {
		fldName := dec.ReadString()
		val, err := dec.readValue(s.format())

		if err != nil {
			return err
		}

		err = cb(fldName, val)

		if err != nil {
			return err
		}
	}

	return nil
}

type structPartCallbacks interface {
	name(ctx context.Context, n string)
	count(c uint64)
	fieldName(n string)
	fieldValue(ctx context.Context, v Value) error
	end()
}

func (s Struct) iterParts(ctx context.Context, cbs structPartCallbacks) error {
	dec := s.decoder()
	dec.skipKind()
	cbs.name(ctx, dec.ReadString())
	count := dec.readCount()
	cbs.count(count)
	for i := uint64(0); i < count; i++ {
		cbs.fieldName(dec.ReadString())
		val, err := dec.readValue(s.format())

		if err != nil {
			return err
		}

		err = cbs.fieldValue(ctx, val)

		if err != nil {
			return err
		}
	}
	cbs.end()
	return nil
}

// MaybeGet returns the value of a field in the struct. If the struct does not a have a field with
// the name name then this returns (nil, false).
func (s Struct) MaybeGet(n string) (v Value, found bool, err error) {
	dec, count := s.decoderSkipToFields()
	for i := uint64(0); i < count; i++ {
		name := dec.ReadString()
		if name == n {
			found = true
			v, err = dec.readValue(s.format())

			if err != nil {
				return nil, false, err
			}

			return v, found, nil
		}

		if name > n {
			return
		}

		err = dec.SkipValue(s.format())

		if err != nil {
			return nil, false, err
		}
	}

	return v, found, nil
}

// Set returns a new struct where the field name has been set to value. If name is not an
// existing field in the struct or the type of value is different from the old value of the
// struct field a new struct type is created.
func (s Struct) Set(n string, v Value) (Struct, error) {
	verifyFieldName(n)

	prolog, head, tail, count, found, err := s.splitFieldsAt(n)

	if err != nil {
		return EmptyStruct(s.Format()), err
	}

	w := binaryNomsWriter{make([]byte, len(s.buff)), 0}
	w.writeRaw(prolog)

	if !found {
		count++
	}
	w.writeCount(count)
	w.writeRaw(head)
	w.writeString(n)
	err = v.writeTo(&w, s.format())

	if err != nil {
		return EmptyStruct(s.Format()), err
	}

	w.writeRaw(tail)

	return Struct{valueImpl{s.vrw, s.format(), w.data(), nil}}, nil
}

// splitFieldsAt splits the buffer into two parts. The fields coming before the field we are looking for
// and the fields coming after it.
func (s Struct) splitFieldsAt(name string) (prolog, head, tail []byte, count uint64, found bool, err error) {
	dec := s.decoder()
	dec.skipKind()
	dec.skipString()
	prolog = dec.buff[:dec.offset]
	count = dec.readCount()
	fieldsOffset := dec.offset

	for i := uint64(0); i < count; i++ {
		beforeCurrent := dec.offset
		fn := dec.ReadString()
		err = dec.SkipValue(s.format())

		if err != nil {
			return nil, nil, nil, 0, false, err
		}

		if fn == name {
			found = true
			head = dec.buff[fieldsOffset:beforeCurrent]
			tail = dec.buff[dec.offset:len(dec.buff)]
			break
		}

		if name < fn {
			head = dec.buff[fieldsOffset:beforeCurrent]
			tail = dec.buff[beforeCurrent:len(dec.buff)]
			break
		}
	}

	if head == nil && tail == nil {
		head = dec.buff[fieldsOffset:dec.offset]
	}

	return prolog, head, tail, count, found, nil
}

// Delete returns a new struct where the field name has been removed.
// If name is not an existing field in the struct then the current struct is returned.
func (s Struct) Delete(n string) (Struct, error) {
	prolog, head, tail, count, found, err := s.splitFieldsAt(n)

	if err != nil {
		return EmptyStruct(s.nbf), err
	}

	if !found {
		return s, nil
	}

	w := binaryNomsWriter{make([]byte, len(s.buff)), 0}
	w.writeRaw(prolog)
	w.writeCount(count - 1)
	w.writeRaw(head)
	w.writeRaw(tail)

	return Struct{valueImpl{s.vrw, s.format(), w.data(), nil}}, nil
}

func (s Struct) Diff(ctx context.Context, last Struct, changes chan<- ValueChanged) error {
	if s.Equals(last) {
		return nil
	}
	dec1, dec2 := s.decoder(), last.decoder()
	dec1.skipKind()
	dec2.skipKind()
	dec1.skipString() // Ignore names
	dec2.skipString()
	count1, count2 := dec1.readCount(), dec2.readCount()
	i1, i2 := uint64(0), uint64(0)
	var fn1, fn2 string

	for i1 < count1 && i2 < count2 {
		if fn1 == "" {
			fn1 = dec1.ReadString()
		}
		if fn2 == "" {
			fn2 = dec2.ReadString()
		}
		var change ValueChanged
		if fn1 == fn2 {
			v1, err := dec1.readValue(s.format())

			if err != nil {
				return err
			}

			v2, err := dec2.readValue(s.format())

			if err != nil {
				return err
			}

			if !v1.Equals(v2) {
				change = ValueChanged{DiffChangeModified, String(fn1), v2, v1}
			}
			i1++
			i2++
			fn1, fn2 = "", ""
		} else if fn1 < fn2 {
			v1, err := dec1.readValue(s.format())

			if err != nil {
				return err
			}

			change = ValueChanged{DiffChangeAdded, String(fn1), nil, v1}
			i1++
			fn1 = ""
		} else {
			v2, err := dec2.readValue(s.format())

			if err != nil {
				return err
			}

			change = ValueChanged{DiffChangeRemoved, String(fn2), v2, nil}
			i2++
			fn2 = ""
		}

		if change != (ValueChanged{}) {
			if err := sendChange(ctx, changes, change); err != nil {
				return err
			}
		}
	}

	for ; i1 < count1; i1++ {
		if fn1 == "" {
			fn1 = dec1.ReadString()
			fmt.Println(fn1)
		}
		v1, err := dec1.readValue(s.format())

		if err != nil {
			return err
		}

		if err := sendChange(ctx, changes, ValueChanged{DiffChangeAdded, String(fn1), nil, v1}); err != nil {
			return err
		}
	}

	for ; i2 < count2; i2++ {
		if fn2 == "" {
			fn2 = dec2.ReadString()
		}

		v2, err := dec2.readValue(s.format())

		if err != nil {
			return err
		}

		if err := sendChange(ctx, changes, ValueChanged{DiffChangeRemoved, String(fn2), v2, nil}); err != nil {
			return err
		}
	}

	return nil
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

func (s Struct) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (s Struct) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (s Struct) String() string {
	panic("unreachable")
}

func (s Struct) HumanReadableString() string {
	panic("unreachable")
}
