// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"bytes"
	"io"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

const (
	addPrefix = "+   "
	subPrefix = "-   "
)

func isPrimitiveOrRef(v1 types.Value) bool {
	kind := v1.Type().Kind()
	return types.IsPrimitiveKind(kind) || kind == types.RefKind
}

func canCompare(v1, v2 types.Value) bool {
	return !isPrimitiveOrRef(v1) && v1.Type().Kind() == v2.Type().Kind()
}

func Diff(w io.Writer, v1, v2 types.Value) (err error) {
	dq := NewDiffQueue()
	di := diffInfo{path: types.NewPath().AddField("/"), v1: v1, v2: v2}
	dq.PushBack(di)

	err = d.Try(func() {
		for di, ok := dq.PopFront(); ok; di, ok = dq.PopFront() {
			p, key, v1, v2 := di.path, di.key, di.v1, di.v2

			v1.Type().Kind()
			if v1 == nil && v2 != nil {
				line(w, addPrefix, key, v2)
			}
			if v1 != nil && v2 == nil {
				line(w, subPrefix, key, v1)
			}
			if !v1.Equals(v2) {
				if !canCompare(v1, v2) {
					line(w, subPrefix, key, v1)
					line(w, addPrefix, key, v2)
				} else {
					switch v1.Type().Kind() {
					case types.ListKind:
						diffLists(dq, w, p, v1.(types.List), v2.(types.List))
					case types.MapKind:
						diffMaps(dq, w, p, v1.(types.Map), v2.(types.Map))
					case types.SetKind:
						diffSets(dq, w, p, v1.(types.Set), v2.(types.Set))
					case types.StructKind:
						diffStructs(dq, w, p, v1.(types.Struct), v2.(types.Struct))
					default:
						panic("Unrecognized type in diff function")
					}
				}
			}
		}
	})
	return
}

func diffLists(dq *diffQueue, w io.Writer, p types.Path, v1, v2 types.List) {
	wroteHeader := false
	splices, _ := v2.Diff(v1)
	for _, splice := range splices {
		if splice.SpRemoved == splice.SpAdded {
			for i := uint64(0); i < splice.SpRemoved; i++ {
				lastEl := v1.Get(splice.SpAt + i)
				newEl := v2.Get(splice.SpFrom + i)
				if canCompare(lastEl, newEl) {
					idx := types.Number(splice.SpAt + i)
					p1 := p.AddIndex(idx)
					dq.PushBack(diffInfo{p1, idx, lastEl, newEl})
				} else {
					wroteHeader = writeHeader(w, wroteHeader, p)
					line(w, subPrefix, nil, v1.Get(splice.SpAt+i))
					line(w, addPrefix, nil, v2.Get(splice.SpFrom+i))
				}
			}
		} else {
			for i := uint64(0); i < splice.SpRemoved; i++ {
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, subPrefix, nil, v1.Get(splice.SpAt+i))
			}
			for i := uint64(0); i < splice.SpAdded; i++ {
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, addPrefix, nil, v2.Get(splice.SpFrom+i))
			}
		}
	}
	writeFooter(w, wroteHeader)
}

func diffMaps(dq *diffQueue, w io.Writer, p types.Path, v1, v2 types.Map) {
	wroteHeader := false

	added, removed, modified := v2.Diff(v1)
	for _, k := range added {
		wroteHeader = writeHeader(w, wroteHeader, p)
		line(w, addPrefix, k, v2.Get(k))
	}
	for _, k := range removed {
		wroteHeader = writeHeader(w, wroteHeader, p)
		line(w, subPrefix, k, v1.Get(k))
	}
	for _, k := range modified {
		c1, c2 := v1.Get(k), v2.Get(k)
		if canCompare(c1, c2) {
			buf := bytes.NewBuffer(nil)
			d.Exp.NoError(types.WriteEncodedValueWithTags(buf, k))
			p1 := p.AddField(buf.String())
			dq.PushBack(diffInfo{path: p1, key: k, v1: c1, v2: c2})
		} else {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, k, v1.Get(k))
			line(w, addPrefix, k, v2.Get(k))
		}
	}
	writeFooter(w, wroteHeader)
}

func diffStructs(dq *diffQueue, w io.Writer, p types.Path, v1, v2 types.Struct) {
	changed := types.StructDiff(v1, v2)
	wroteHeader := false
	for _, field := range changed {
		f1 := v1.Get(field)
		f2 := v2.Get(field)
		if canCompare(f1, f2) {
			p1 := p.AddField(field)
			dq.PushBack(diffInfo{path: p1, key: types.NewString(field), v1: f1, v2: f2})
		} else {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, types.NewString(field), f1)
			line(w, addPrefix, types.NewString(field), f2)
		}
	}
}

func diffSets(dq *diffQueue, w io.Writer, p types.Path, v1, v2 types.Set) {
	wroteHeader := false
	added, removed := v2.Diff(v1)
	if len(added) == 1 && len(removed) == 1 && canCompare(added[0], removed[0]) {
		p1 := p.AddField(added[0].Hash().String())
		dq.PushBack(diffInfo{path: p1, key: types.NewString(""), v1: removed[0], v2: added[0]})
	} else {
		for _, value := range removed {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, nil, value)
		}
		for _, value := range added {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, addPrefix, nil, value)
		}
	}
	writeFooter(w, wroteHeader)
	return
}

type prefixWriter struct {
	w      io.Writer
	prefix []byte
}

// todo: Not sure if we want to use a writer to do this for the longterm but, if so, we can
// probably do better than writing byte by byte
func (pw prefixWriter) Write(bytes []byte) (n int, err error) {
	for i, b := range bytes {
		_, err = pw.w.Write([]byte{b})
		if err != nil {
			return i, err
		}
		if b == '\n' {
			_, err := pw.w.Write(pw.prefix)
			if err != nil {
				return i, err
			}
		}
	}
	return len(bytes), nil
}

func line(w io.Writer, start string, key, v2 types.Value) {
	var err error
	pw := prefixWriter{w: w, prefix: []byte(start)}
	_, err = w.Write([]byte(start))
	d.Exp.NoError(err)
	if key != nil {
		d.Exp.NoError(types.WriteEncodedValueWithTags(pw, key))
		_, err = w.Write([]byte(": "))
		d.Exp.NoError(err)
	}
	d.Exp.NoError(types.WriteEncodedValueWithTags(pw, v2))
	_, err = w.Write([]byte("\n"))
	d.Exp.NoError(err)
}

func writeHeader(w io.Writer, wroteHeader bool, p types.Path) bool {
	var err error
	if !wroteHeader {
		_, err = w.Write([]byte(p.String()))
		d.Exp.NoError(err)
		_, err = w.Write([]byte(" {\n"))
		d.Exp.NoError(err)
		wroteHeader = true
	}
	return wroteHeader
}

func writeFooter(w io.Writer, wroteHeader bool) {
	if wroteHeader {
		_, err := w.Write([]byte("  }\n"))
		d.Exp.NoError(err)
	}
}
