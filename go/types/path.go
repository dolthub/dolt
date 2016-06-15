// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/go/d"
)

type Path []pathPart

type pathPart interface {
	Resolve(v Value) Value
	String() string
}

func NewPath() Path {
	return Path{}
}

func (p Path) AddField(name string) Path {
	p1 := make(Path, len(p), len(p)+1)
	copy(p1, p)
	return append(p1, newFieldPart(name))
}

func (p Path) AddIndex(idx Value) Path {
	p1 := make(Path, len(p), len(p)+1)
	copy(p1, p)
	return append(p1, newIndexPart(idx))
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
			return Path{}, errors.New("Invalid field " + tail)
		}

		return p.AddField(tail[:idx[1]]).addPath(tail[idx[1]:])

	case '[':
		if len(tail) == 0 {
			return Path{}, errors.New("Path ends in [")
		}

		idx, rem, err := parsePathIndex(tail)
		if err != nil {
			return Path{}, err
		}

		return p.AddIndex(idx).addPath(rem)

	case ']':
		return Path{}, errors.New("] is missing opening [")

	default:
		return Path{}, errors.New(fmt.Sprintf("%c is not a valid operator", op))
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
	strs := make([]string, len(p))
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
}

func newIndexPart(idx Value) indexPart {
	k := idx.Type().Kind()
	d.Chk.True(k == StringKind || k == BoolKind || k == NumberKind)
	return indexPart{idx}
}

func (ip indexPart) Resolve(v Value) Value {
	if l, ok := v.(List); ok {
		if n, ok := ip.idx.(Number); ok {
			f := float64(n)
			if f == math.Trunc(f) && f >= 0 {
				u := uint64(f)
				if u < l.Len() {
					return l.Get(u)
				}
			}
		}
	}

	if m, ok := v.(Map); ok {
		return m.Get(ip.idx)
	}

	return nil
}

func (ip indexPart) String() string {
	return fmt.Sprintf("[%s]", EncodedValue(ip.idx))
}

func parsePathIndex(str string) (idx Value, rem string, err error) {
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
			idx = NewString(stringBuf.String())
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

		if idxStr == "true" {
			idx = Bool(true)
		} else if idxStr == "false" {
			idx = Bool(false)
		} else if i, err2 := strconv.ParseFloat(idxStr, 64); err2 == nil {
			// Should we be more strict here? ParseFloat allows leading and trailing dots, and exponents.
			idx = Number(i)
		} else {
			err = errors.New("Invalid index " + idxStr)
		}
	}

	return
}
