// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

var annotationRe = regexp.MustCompile("^@([a-z]+)")

// A Path is an address to a Noms value - and unlike hashes (i.e. #abcd...) they can address inlined values.
// See https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
type Path []PathPart

type PathPart interface {
	Resolve(v Value) Value
	String() string
}

func ParsePath(str string) (Path, error) {
	if str == "" {
		return Path{}, errors.New("Empty path")
	}
	return constructPath(Path{}, str)
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
			return Path{}, errors.New("Invalid field: " + tail)
		}
		p = append(p, FieldPath{tail[:idx[1]]})
		return constructPath(p, tail[idx[1]:])

	case '[':
		if len(tail) == 0 {
			return Path{}, errors.New("Path ends in [")
		}

		idx, h, rem, err := ParsePathIndex(tail)
		if err != nil {
			return Path{}, err
		}
		if !strings.HasPrefix(rem, "]") {
			return Path{}, errors.New("[ is missing closing ]")
		}
		rem = rem[1:]

		intoKey := false
		if ann, rem2 := getAnnotation(rem); ann != "" {
			if ann != "key" {
				return Path{}, fmt.Errorf("Unsupported annotation: @%s", ann)
			}
			intoKey = true
			rem = rem2
		}

		d.Chk.NotEqual(idx == nil, h.IsEmpty())

		var part PathPart
		switch {
		case idx != nil && intoKey:
			part = NewIndexIntoKeyPath(idx)
		case idx != nil:
			part = NewIndexPath(idx)
		case intoKey:
			part = NewHashIndexIntoKeyPath(h)
		default:
			part = NewHashIndexPath(h)
		}
		p = append(p, part)
		return constructPath(p, rem)

	case ']':
		return Path{}, errors.New("] is missing opening [")

	default:
		return Path{}, fmt.Errorf("Invalid operator: %c", op)
	}
}

func (p Path) Resolve(v Value) (resolved Value) {
	resolved = v
	for _, part := range p {
		if resolved == nil {
			break
		}
		resolved = part.Resolve(resolved)
	}

	return
}

func (p Path) String() string {
	strs := make([]string, 0, len(p))
	for _, part := range p {
		strs = append(strs, part.String())
	}
	return strings.Join(strs, "")
}

// Gets Struct field values by name.
type FieldPath struct {
	// The name of the field, e.g. `.Name`.
	Name string
}

func NewFieldPath(name string) FieldPath {
	return FieldPath{name}
}

func (fp FieldPath) Resolve(v Value) Value {
	if s, ok := v.(Struct); ok {
		if fv, ok := s.MaybeGet(fp.Name); ok {
			return fv
		}
	}

	return nil
}

func (fp FieldPath) String() string {
	return fmt.Sprintf(".%s", fp.Name)
}

// Indexes into Maps and Lists by key or index.
type IndexPath struct {
	// The value of the index, e.g. `[42]` or `["value"]`.
	Index Value
	// Whether this index should resolve to the key of a map, given by a `@key` annotation.
	// Typically IntoKey is false, and indices would resolve to the values. E.g. given `{a: 42}` then `["a"]` resolves to `42`.
	// If IntoKey is true, then it resolves to `"a"`. For IndexPath this isn't particularly useful - it's mostly provided for consistency with HashIndexPath - but note that given `{a: 42}` then `["b"]` resolves to nil, not `"b"`.
	IntoKey bool
}

func NewIndexPath(idx Value) IndexPath {
	return newIndexPath(idx, false)
}

func NewIndexIntoKeyPath(idx Value) IndexPath {
	return newIndexPath(idx, true)
}

func newIndexPath(idx Value, intoKey bool) IndexPath {
	k := idx.Type().Kind()
	d.Chk.True(k == StringKind || k == BoolKind || k == NumberKind)
	return IndexPath{idx, intoKey}
}

func (ip IndexPath) Resolve(v Value) Value {
	switch v := v.(type) {
	case List:
		if n, ok := ip.Index.(Number); ok {
			f := float64(n)
			if f == math.Trunc(f) && f >= 0 {
				u := uint64(f)
				if u < v.Len() {
					if ip.IntoKey {
						return ip.Index
					}
					return v.Get(u)
				}
			}
		}

	case Map:
		if ip.IntoKey && v.Has(ip.Index) {
			return ip.Index
		}
		if !ip.IntoKey {
			return v.Get(ip.Index)
		}
	}

	return nil
}

func (ip IndexPath) String() (str string) {
	ann := ""
	if ip.IntoKey {
		ann = "@key"
	}
	return fmt.Sprintf("[%s]%s", EncodedIndexValue(ip.Index), ann)
}

// Indexes into Maps by the hash of a key, or a Set by the hash of a value.
type HashIndexPath struct {
	// The hash of the key or value to search for. Maps and Set are ordered, so this in O(log(size)).
	Hash hash.Hash
	// Whether this index should resolve to the key of a map, given by a `@key` annotation.
	// Typically IntoKey is false, and indices would resolve to the values. E.g. given `{a: 42}` and if the hash of `"a"` is `#abcd`, then `[#abcd]` resolves to `42`.
	// If IntoKey is true, then it resolves to `"a"`. This is useful for when Map keys aren't primitive values, e.g. a struct, since struct literals can't be spelled using a Path.
	IntoKey bool
}

func NewHashIndexPath(h hash.Hash) HashIndexPath {
	return newHashIndexPath(h, false)
}

func NewHashIndexIntoKeyPath(h hash.Hash) HashIndexPath {
	return newHashIndexPath(h, true)
}

func newHashIndexPath(h hash.Hash, intoKey bool) HashIndexPath {
	d.Chk.False(h.IsEmpty())
	return HashIndexPath{h, intoKey}
}

func (hip HashIndexPath) Resolve(v Value) (res Value) {
	var seq orderedSequence
	var getCurrentValue func(cur *sequenceCursor) Value

	switch v := v.(type) {
	case Set:
		// Unclear what the behavior should be if |hip.IntoKey| is true, but ignoring it for sets is arguably correct.
		seq = v.seq
		getCurrentValue = func(cur *sequenceCursor) Value { return cur.current().(Value) }
	case Map:
		seq = v.seq
		if hip.IntoKey {
			getCurrentValue = func(cur *sequenceCursor) Value { return cur.current().(mapEntry).key }
		} else {
			getCurrentValue = func(cur *sequenceCursor) Value { return cur.current().(mapEntry).value }
		}
	default:
		return nil
	}

	cur := newCursorAt(seq, orderedKeyFromHash(hip.Hash), false, false)
	if !cur.valid() {
		return nil
	}

	if getCurrentKey(cur).h != hip.Hash {
		return nil
	}

	return getCurrentValue(cur)
}

func (hip HashIndexPath) String() string {
	ann := ""
	if hip.IntoKey {
		ann = "@key"
	}
	return fmt.Sprintf("[#%s]%s", hip.Hash.String(), ann)
}

// Parse a Noms value from the path index syntax.
// 4 ->          types.Number
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
					err = errors.New(`Only " and \ can be escaped`)
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
			err = errors.New("Empty index value")
		} else if idxStr[0] == '#' {
			hashStr := idxStr[1:]
			h, _ = hash.MaybeParse(hashStr)
			if h.IsEmpty() {
				err = errors.New("Invalid hash: " + hashStr)
			}
		} else if idxStr == "true" {
			idx = Bool(true)
		} else if idxStr == "false" {
			idx = Bool(false)
		} else if i, err2 := strconv.ParseFloat(idxStr, 64); err2 == nil {
			// Should we be more strict here? ParseFloat allows leading and trailing dots, and exponents.
			idx = Number(i)
		} else {
			err = errors.New("Invalid index: " + idxStr)
		}
	}

	return
}

func getAnnotation(str string) (ann, rem string) {
	if parts := annotationRe.FindStringSubmatch(str); parts != nil {
		ann = parts[1]
		rem = str[len(parts[0]):]
	}
	return
}
