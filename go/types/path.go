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

type Path []pathPart

type pathPart interface {
	Resolve(v Value) Value
	String() string
}

func NewPath() Path {
	return Path{}
}

func ParsePath(path string) (Path, error) {
	return NewPath().AddPath(path)
}

func (p Path) AddField(name string) Path {
	return p.appendPart(newFieldPart(name))
}

func (p Path) AddIndex(idx Value) Path {
	return p.appendPart(newIndexPart(idx, false))
}

func (p Path) AddKeyIndex(idx Value) Path {
	return p.appendPart(newIndexPart(idx, true))
}

func (p Path) AddHashIndex(h hash.Hash) Path {
	return p.appendPart(newHashIndexPart(h, false))
}

func (p Path) AddHashKeyIndex(h hash.Hash) Path {
	return p.appendPart(newHashIndexPart(h, true))
}

func (p Path) appendPart(part pathPart) Path {
	p2 := make([]pathPart, len(p), len(p)+1)
	copy(p2, p)
	return append(p2, part)
}

func (p Path) AddPath(str string) (Path, error) {
	if len(str) == 0 {
		return Path{}, errors.New("Empty path")
	}

	return p.addPath(str)
}

func (p Path) addPath(str string) (Path, error) {
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

		return p.AddField(tail[:idx[1]]).addPath(tail[idx[1]:])

	case '[':
		if len(tail) == 0 {
			return Path{}, errors.New("Path ends in [")
		}

		idx, h, rem, err := parsePathIndex(tail)
		if err != nil {
			return Path{}, err
		}

		key := false
		if annParts := annotationRe.FindStringSubmatch(rem); annParts != nil {
			ann := annParts[1]
			if ann != "key" {
				return Path{}, fmt.Errorf("Unsupported annotation: @%s", ann)
			}
			key = true
			rem = rem[len(annParts[0]):]
		}

		d.Chk.NotEqual(idx == nil, h.IsEmpty())

		switch {
		case idx != nil && key:
			return p.AddKeyIndex(idx).addPath(rem)
		case idx != nil:
			return p.AddIndex(idx).addPath(rem)
		case key:
			return p.AddHashKeyIndex(h).addPath(rem)
		default:
			return p.AddHashIndex(h).addPath(rem)
		}

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

type fieldPart struct {
	name string
}

func newFieldPart(name string) fieldPart {
	return fieldPart{name}
}

func (fp fieldPart) Resolve(v Value) Value {
	if s, ok := v.(Struct); ok {
		if fv, ok := s.MaybeGet(fp.name); ok {
			return fv
		}
	}

	return nil
}

func (fp fieldPart) String() string {
	return fmt.Sprintf(".%s", fp.name)
}

type indexPart struct {
	idx Value
	key bool
}

func newIndexPart(idx Value, key bool) indexPart {
	k := idx.Type().Kind()
	d.Chk.True(k == StringKind || k == BoolKind || k == NumberKind)
	return indexPart{idx, key}
}

func (ip indexPart) Resolve(v Value) Value {
	switch v := v.(type) {
	case List:
		if n, ok := ip.idx.(Number); ok {
			f := float64(n)
			if f == math.Trunc(f) && f >= 0 {
				u := uint64(f)
				if u < v.Len() {
					if ip.key {
						return ip.idx
					}
					return v.Get(u)
				}
			}
		}

	case Map:
		if ip.key && v.Has(ip.idx) {
			return ip.idx
		}
		if !ip.key {
			return v.Get(ip.idx)
		}
	}

	return nil
}

func (ip indexPart) String() (str string) {
	ann := ""
	if ip.key {
		ann = "@key"
	}
	return fmt.Sprintf("[%s]%s", EncodedIndexValue(ip.idx), ann)
}

type hashIndexPart struct {
	h   hash.Hash
	key bool
}

func newHashIndexPart(h hash.Hash, key bool) hashIndexPart {
	return hashIndexPart{h, key}
}

func (hip hashIndexPart) Resolve(v Value) (res Value) {
	var seq orderedSequence
	var getCurrentValue func(cur *sequenceCursor) Value

	switch v := v.(type) {
	case Set:
		// Unclear what the behavior should be if |hip.key| is true, but ignoring it for sets is arguably correct.
		seq = v.seq
		getCurrentValue = func(cur *sequenceCursor) Value { return cur.current().(Value) }
	case Map:
		seq = v.seq
		if hip.key {
			getCurrentValue = func(cur *sequenceCursor) Value { return cur.current().(mapEntry).key }
		} else {
			getCurrentValue = func(cur *sequenceCursor) Value { return cur.current().(mapEntry).value }
		}
	default:
		return nil
	}

	cur := newCursorAt(seq, orderedKeyFromHash(hip.h), false, false)
	if !cur.valid() {
		return nil
	}

	if getCurrentKey(cur).h != hip.h {
		return nil
	}

	return getCurrentValue(cur)
}

func (hip hashIndexPart) String() string {
	ann := ""
	if hip.key {
		ann = "@key"
	}
	return fmt.Sprintf("[#%s]%s", hip.h.String(), ann)
}

func parsePathIndex(str string) (idx Value, h hash.Hash, rem string, err error) {
Switch:
	switch str[0] {
	case '"':
		// String is complicated because ] might be quoted, and " or \ might be escaped.
		stringBuf := bytes.Buffer{}
		i := 1

		for ; i < len(str); i++ {
			c := str[i]
			if c == '"' {
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

		if i == len(str) {
			err = errors.New("[ is missing closing ]")
		} else {
			idx = String(stringBuf.String())
			rem = str[i+2:]
		}

	default:
		split := strings.SplitN(str, "]", 2)
		if len(split) < 2 {
			err = errors.New("[ is missing closing ]")
			break Switch
		}

		idxStr := split[0]
		rem = split[1]

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
