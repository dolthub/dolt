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
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

// For an annotation like @type, 1st capture group is the annotation.
// For @at(42), 1st capture group is the annotation and 3rd is the parameter.
// Note, @at() is valid under this regexp, code should deal with the error.
var annotationRe = regexp.MustCompile(`^([a-z]+)(\(([\w\-"']*)\))?`)

// A Path locates a value in Noms relative to some other value. For locating
// values absolutely within a database, see AbsolutePath. To locate values
// globally, see Spec.
//
// For more details, see:
// https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
type Path []PathPart

type PathPart interface {
	Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error)
	String() string
}

// ParsePath parses str into a Path, or returns an error if parsing failed.
func ParsePath(str string) (Path, error) {
	if str == "" {
		return Path{}, errors.New("empty path")
	}
	return constructPath(Path{}, str)
}

// MustParsePath parses str into a Path, or panics if parsing failed.
func MustParsePath(str string) Path {
	p, err := ParsePath(str)
	if err != nil {
		panic(err)
	}
	return p
}

type keyIndexable interface {
	setIntoKey(v bool) keyIndexable
}

func constructPath(p Path, str string) (Path, error) {
	if len(str) == 0 {
		return p, nil
	}

	op, tail := str[0], str[1:]

	switch op {
	case '.':
		idx := fieldNameComponentRe.FindIndex([]byte(tail))
		if idx == nil {
			return Path{}, errors.New("invalid field: " + tail)
		}
		p = append(p, FieldPath{tail[:idx[1]]})
		return constructPath(p, tail[idx[1]:])

	case '[':
		if len(tail) == 0 {
			return Path{}, errors.New("path ends in [")
		}

		idx, h, rem, err := ParsePathIndex(tail)
		if err != nil {
			return Path{}, err
		}
		if !strings.HasPrefix(rem, "]") {
			return Path{}, errors.New("[ is missing closing ]")
		}
		d.PanicIfTrue(idx == nil && h.IsEmpty())
		d.PanicIfTrue(idx != nil && !h.IsEmpty())

		if idx != nil {
			p = append(p, NewIndexPath(idx))
		} else {
			p = append(p, NewHashIndexPath(h))
		}
		return constructPath(p, rem[1:])

	case '@':
		ann, hasArg, arg, rem := getAnnotation(tail)

		switch ann {
		case "at":
			if arg == "" {
				return Path{}, fmt.Errorf("@at annotation requires a position argument")
			}
			idx, err := strconv.ParseInt(arg, 10, 64)
			if err != nil {
				return Path{}, fmt.Errorf("invalid position: %s", arg)
			}
			return constructPath(append(p, NewAtAnnotation(idx)), rem)

		case "key":
			if hasArg {
				return Path{}, fmt.Errorf("@key annotation does not support arguments")
			}
			if len(p) == 0 {
				return Path{}, fmt.Errorf("cannot use @key annotation at beginning of path")
			}
			lastPart := p[len(p)-1]
			if ki, ok := lastPart.(keyIndexable); ok {
				p[len(p)-1] = ki.setIntoKey(true).(PathPart)
				return constructPath(p, rem)
			}
			return Path{}, fmt.Errorf("cannot use @key annotation on: %s", lastPart.String())

		case "target":
			if hasArg {
				return Path{}, fmt.Errorf("@target annotation does not support arguments")
			}
			return constructPath(append(p, TargetAnnotation{}), rem)

		case "type":
			if hasArg {
				return Path{}, fmt.Errorf("@type annotation does not support arguments")
			}
			return constructPath(append(p, TypeAnnotation{}), rem)

		default:
			return Path{}, fmt.Errorf("unsupported annotation: @%s", ann)
		}

	case ']':
		return Path{}, errors.New("] is missing opening [")

	default:
		return Path{}, fmt.Errorf("invalid operator: %c", op)
	}
}

// Resolve resolves a path relative to some value.
// A ValueReader is required to resolve paths that contain the @target annotation.
func (p Path) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	resolved := v
	for _, part := range p {
		if resolved == nil {
			break
		}

		var err error
		resolved, err = part.Resolve(ctx, resolved, vr)

		if err != nil {
			return nil, err
		}
	}

	return resolved, nil
}

func (p Path) Equals(o Path) bool {
	if len(p) != len(o) {
		return false
	}
	for i, pp := range p {
		if pp != o[i] {
			return false
		}
	}
	return true
}

// Append makes a copy of a p and appends the PathPart 'pp' to it.
func (p Path) Append(pp PathPart) Path {
	p1 := make(Path, len(p), len(p)+1)
	copy(p1, p)
	return append(p1, pp)
}

func (p Path) String() string {
	strs := make([]string, 0, len(p))
	for _, part := range p {
		strs = append(strs, part.String())
	}
	return strings.Join(strs, "")
}

func (p Path) IsEmpty() bool {
	return len(p) == 0
}

// FieldPath references Struct field values by name.
type FieldPath struct {
	// The name of the field, e.g. `.Name`.
	Name string
}

func NewFieldPath(name string) FieldPath {
	return FieldPath{name}
}

func (fp FieldPath) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	switch v := v.(type) {
	case Struct:
		if sv, ok, err := v.MaybeGet(fp.Name); err != nil {
			return nil, err
		} else if ok {
			return sv, nil
		}
	case SerialMessage:
		data := []byte(v)
		if serial.GetFileID(data) == serial.CommitFileID && fp.Name == "value" {
			msg := serial.GetRootAsCommit(data, 0)
			addr := hash.New(msg.RootBytes())
			return vr.ReadValue(ctx, addr)
		}
	case *Type:
		if desc, ok := v.Desc.(StructDesc); ok {
			if df, _ := desc.Field(fp.Name); df != nil {
				return df, nil
			}
		}
	}
	return nil, nil
}

func (fp FieldPath) String() string {
	return fmt.Sprintf(".%s", fp.Name)
}

// IndexPath ndexes into Maps and Lists by key or index.
type IndexPath struct {
	// The value of the index, e.g. `[42]` or `["value"]`. If Index is a negative
	// number and the path is resolved in a List, it means index from the back.
	Index Value
	// Whether this index should resolve to the key of a map, given by a `@key`
	// annotation. Typically IntoKey is false, and indices would resolve to the
	// values. E.g. given `{a: 42}` then `["a"]` resolves to `42`. If IntoKey is
	// true, then it resolves to `"a"`. For IndexPath this isn't particularly
	// useful - it's mostly provided for consistency with HashIndexPath - but
	// note that given `{a: 42}` then `["b"]` resolves to nil, not `"b"`.
	IntoKey bool
}

func NewIndexPath(idx Value) IndexPath {
	return newIndexPath(idx, false)
}

func ValueCanBePathIndex(v Value) bool {
	k := v.Kind()
	return k == StringKind || k == BoolKind || k == FloatKind
}

func newIndexPath(idx Value, intoKey bool) IndexPath {
	d.PanicIfFalse(ValueCanBePathIndex(idx))
	return IndexPath{idx, intoKey}
}

func (ip IndexPath) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	seqIndex := func(getter func(i uint64) (Value, error)) (Value, error) {
		n, ok := ip.Index.(Float)
		if !ok {
			return nil, nil
		}
		f := float64(n)
		if f != math.Trunc(f) {
			return nil, nil
		}
		ai, ok := getAbsoluteIndex(v, int64(f))
		if !ok {
			return nil, nil
		}
		if ip.IntoKey {
			return Float(ai), nil
		}
		return getter(ai)
	}

	switch v := v.(type) {
	case List:
		return seqIndex(func(i uint64) (Value, error) { return v.Get(ctx, i) })
	case *Type:
		if cd, ok := v.Desc.(CompoundDesc); ok {
			return seqIndex(func(i uint64) (Value, error) { return cd.ElemTypes[i], nil })
		}
	case Map:
		if !ip.IntoKey {
			v, _, err := v.MaybeGet(ctx, ip.Index)
			return v, err
		}

		if has, err := v.Has(ctx, ip.Index); err != nil {
			return nil, err
		} else if has {
			return ip.Index, nil
		}
	}

	return nil, nil
}

func (ip IndexPath) String() (str string) {
	valStr, err := EncodedIndexValue(context.Background(), ip.Index)

	if err != nil {
		return "error: " + err.Error()
	}

	str = fmt.Sprintf("[%s]", valStr)
	if ip.IntoKey {
		str += "@key"
	}
	return
}

func (ip IndexPath) setIntoKey(v bool) keyIndexable {
	ip.IntoKey = v
	return ip
}

// Indexes into Maps by the hash of a key, or a Set by the hash of a value.
type HashIndexPath struct {
	// The hash of the key or value to search for. Maps and Set are ordered, so
	// this in O(log(size)).
	Hash hash.Hash
	// Whether this index should resolve to the key of a map, given by a `@key`
	// annotation. Typically IntoKey is false, and indices would resolve to the
	// values. E.g. given `{a: 42}` and if the hash of `"a"` is `#abcd`, then
	// `[#abcd]` resolves to `42`. If IntoKey is true, then it resolves to `"a"`.
	// This is useful for when Map keys aren't primitive values, e.g. a struct,
	// since struct literals can't be spelled using a Path.
	IntoKey bool
}

func NewHashIndexPath(h hash.Hash) HashIndexPath {
	return newHashIndexPath(h, false)
}

func newHashIndexPath(h hash.Hash, intoKey bool) HashIndexPath {
	d.PanicIfTrue(h.IsEmpty())
	return HashIndexPath{h, intoKey}
}

func (hip HashIndexPath) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	var seq orderedSequence
	var getCurrentValue func(cur *sequenceCursor) (Value, error)

	switch v := v.(type) {
	case Set:
		// Unclear what the behavior should be if |hip.IntoKey| is true, but ignoring it for sets is arguably correct.
		seq = v.orderedSequence
		getCurrentValue = func(cur *sequenceCursor) (Value, error) {
			item, err := cur.current()

			if err != nil {
				return nil, err
			}

			return item.(Value), nil
		}
	case Map:
		seq = v.orderedSequence
		if hip.IntoKey {
			getCurrentValue = func(cur *sequenceCursor) (Value, error) {
				item, err := cur.current()

				if err != nil {
					return nil, err
				}

				return item.(mapEntry).key, nil
			}
		} else {
			getCurrentValue = func(cur *sequenceCursor) (Value, error) {
				item, err := cur.current()

				if err != nil {
					return nil, err
				}

				return item.(mapEntry).value, nil
			}
		}
	default:
		return nil, nil
	}

	cur, err := newCursorAt(ctx, seq, orderedKeyFromHash(hip.Hash), false, false)

	if err != nil {
		return nil, err
	}

	if !cur.valid() {
		return nil, nil
	}

	currKey, err := getCurrentKey(cur)

	if err != nil {
		return nil, err
	}

	if currKey.h != hip.Hash {
		return nil, nil
	}

	return getCurrentValue(cur)
}

func (hip HashIndexPath) String() (str string) {
	str = fmt.Sprintf("[#%s]", hip.Hash.String())
	if hip.IntoKey {
		str += "@key"
	}
	return
}

func (hip HashIndexPath) setIntoKey(v bool) keyIndexable {
	hip.IntoKey = v
	return hip
}

// Parse a Noms value from the path index syntax.
// 4 ->          types.Float
// "4" ->        types.String
// true|false -> types.Boolean
// #<chars> ->   hash.Hash
func ParsePathIndex(str string) (idx Value, h hash.Hash, rem string, err error) {
Switch:
	switch str[0] {
	case '"':
		// String is complicated because ] might be quoted, and " or \ might be escaped.
		stringBuf := bytes.Buffer{}
		i := 1

		for ; i < len(str); i++ {
			c := str[i]
			if c == '"' {
				i++
				break
			}
			if c == '\\' && i < len(str)-1 {
				i++
				c = str[i]
				if c != '\\' && c != '"' {
					err = errors.New(`only " and \ can be escaped`)
					break Switch
				}
			}
			stringBuf.WriteByte(c)
		}

		idx = String(stringBuf.String())
		rem = str[i:]

	default:
		idxStr := str
		sepIdx := strings.Index(str, "]")
		if sepIdx >= 0 {
			idxStr = str[:sepIdx]
			rem = str[sepIdx:]
		}
		if len(idxStr) == 0 {
			err = errors.New("empty index value")
		} else if idxStr[0] == '#' {
			hashStr := idxStr[1:]
			h, _ = hash.MaybeParse(hashStr)
			if h.IsEmpty() {
				err = errors.New("invalid hash: " + hashStr)
			}
		} else if idxStr == "true" {
			idx = Bool(true)
		} else if idxStr == "false" {
			idx = Bool(false)
		} else if i, err2 := strconv.ParseFloat(idxStr, 64); err2 == nil {
			// Should we be more strict here? ParseFloat allows leading and trailing dots, and exponents.
			idx = Float(i)
		} else {
			err = errors.New("invalid index: " + idxStr)
		}
	}

	return
}

// TypeAnnotation is a PathPart annotation to resolve to the type of the value
// it's resolved in.
type TypeAnnotation struct {
}

func (ann TypeAnnotation) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	return TypeOf(v)
}

func (ann TypeAnnotation) String() string {
	return "@type"
}

// TargetAnnotation is a PathPart annotation to resolve to the targetValue of the Ref it is resolved on.
type TargetAnnotation struct {
}

func (ann TargetAnnotation) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	if vr == nil {
		d.Panic("@target annotation requires a database to resolve against")
	}
	if r, ok := v.(Ref); ok {
		return r.TargetValue(ctx, vr)
	} else {
		return nil, nil
	}
}

func (ann TargetAnnotation) String() string {
	return "@target"
}

// AtAnnotation is a PathPart annotation that gets the value of a collection at
// a position, rather than a key. This is equivalent to IndexPath for lists,
// but different for sets and maps.
type AtAnnotation struct {
	// Index is the position to resolve at. If negative, it means an index
	// relative to the end of the collection.
	Index int64
	// IntoKey see IndexPath.IntoKey.
	IntoKey bool
}

func NewAtAnnotation(idx int64) AtAnnotation {
	return AtAnnotation{idx, false}
}

func (ann AtAnnotation) Resolve(ctx context.Context, v Value, vr ValueReader) (Value, error) {
	ai, ok := getAbsoluteIndex(v, ann.Index)
	if !ok {
		return nil, nil
	}

	switch v := v.(type) {
	case List:
		if !ann.IntoKey {
			return v.Get(ctx, ai)
		}
	case Set:
		return v.At(ctx, ai)
	case Map:
		k, mapv, err := v.At(ctx, ai)

		if err != nil {
			return nil, err
		}

		if ann.IntoKey {
			return k, nil
		}

		return mapv, nil
	case *Type:
		if cd, ok := v.Desc.(CompoundDesc); ok {
			return cd.ElemTypes[ai], nil
		}
	}

	return nil, nil
}

func (ann AtAnnotation) String() (str string) {
	str = fmt.Sprintf("@at(%d)", ann.Index)
	if ann.IntoKey {
		str += "@key"
	}
	return
}

func (ann AtAnnotation) setIntoKey(v bool) keyIndexable {
	ann.IntoKey = v
	return ann
}

func getAnnotation(str string) (ann string, hasArg bool, arg, rem string) {
	parts := annotationRe.FindStringSubmatch(str)
	if parts == nil {
		return
	}

	ann = parts[1]
	hasArg = parts[2] != ""
	arg = parts[3]
	rem = str[len(parts[0]):]
	return
}

func getAbsoluteIndex(v Value, relIdx int64) (absIdx uint64, ok bool) {
	var l uint64
	switch v := v.(type) {
	case Collection:
		l = v.Len()
	case *Type:
		if cd, cdOK := v.Desc.(CompoundDesc); cdOK {
			l = uint64(len(cd.ElemTypes))
		} else {
			return
		}
	default:
		return
	}

	if relIdx < 0 {
		if uint64(-relIdx) > l {
			return
		}
		absIdx = l - uint64(-relIdx)
	} else {
		if uint64(relIdx) >= l {
			return
		}
		absIdx = uint64(relIdx)
	}

	ok = true
	return
}
