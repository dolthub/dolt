// Copyright 2016 Attic Labs, Inc. All rights reserved.
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
			d.Chk.True(v1 != nil && v2 != nil) // nil is not a valid types.Value and we should never get one
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

	splices := make(chan types.Splice)
	closeChan := make(chan struct{})
	go func() {
		v2.Diff(v1, splices, closeChan)
		close(splices)
	}()

	err := d.Try(func() {
		for splice := range splices {
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
	})
	if err != nil {
		closeChan <- struct{}{}
	}
	writeFooter(w, wroteHeader)
}

func diffMaps(dq *diffQueue, w io.Writer, p types.Path, v1, v2 types.Map) {
	wroteHeader := false

	changes := make(chan types.ValueChanged)
	closeChan := make(chan struct{})
	go func() {
		v2.Diff(v1, changes, closeChan)
		close(changes)
	}()

	err := d.Try(func() {
		for change := range changes {
			switch change.ChangeType {
			case types.DiffChangeAdded:
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, addPrefix, change.V, v2.Get(change.V))
			case types.DiffChangeRemoved:
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, subPrefix, change.V, v1.Get(change.V))
			case types.DiffChangeModified:
				c1, c2 := v1.Get(change.V), v2.Get(change.V)
				if canCompare(c1, c2) {
					buf := bytes.NewBuffer(nil)
					d.PanicIfError(types.WriteEncodedValueWithTags(buf, change.V))
					p1 := p.AddField(buf.String())
					dq.PushBack(diffInfo{path: p1, key: change.V, v1: c1, v2: c2})
				} else {
					wroteHeader = writeHeader(w, wroteHeader, p)
					line(w, subPrefix, change.V, c1)
					line(w, addPrefix, change.V, c2)
				}
			default:
				panic("unknown change type")
			}
		}
	})
	if err != nil {
		closeChan <- struct{}{}
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
			dq.PushBack(diffInfo{path: p1, key: types.String(field), v1: f1, v2: f2})
		} else {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, types.String(field), f1)
			line(w, addPrefix, types.String(field), f2)
		}
	}
}

func diffSets(dq *diffQueue, w io.Writer, p types.Path, v1, v2 types.Set) {
	wroteHeader := false

	changes := make(chan types.ValueChanged)
	closeChan := make(chan struct{})
	go func() {
		v2.Diff(v1, changes, closeChan)
		close(changes)
	}()

	err := d.Try(func() {
		for change := range changes {
			switch change.ChangeType {
			case types.DiffChangeAdded:
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, addPrefix, nil, change.V)
			case types.DiffChangeRemoved:
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, subPrefix, nil, change.V)
			default:
				// sets should not have any DiffChangeModified or unknown change types
				panic("unknown change type")
			}
		}
	})
	if err != nil {
		closeChan <- struct{}{}
	}
	writeFooter(w, wroteHeader)
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
	d.PanicIfError(err)
	if key != nil {
		d.PanicIfError(types.WriteEncodedValue(pw, key))
		_, err = w.Write([]byte(": "))
		d.PanicIfError(err)
	}
	d.PanicIfError((types.WriteEncodedValue(pw, v2)))
	_, err = w.Write([]byte("\n"))
	d.PanicIfError(err)
}

func writeHeader(w io.Writer, wroteHeader bool, p types.Path) bool {
	var err error
	if !wroteHeader {
		_, err = w.Write([]byte(p.String()))
		d.PanicIfError(err)
		_, err = w.Write([]byte(" {\n"))
		d.PanicIfError(err)
		wroteHeader = true
	}
	return wroteHeader
}

func writeFooter(w io.Writer, wroteHeader bool) {
	if wroteHeader {
		_, err := w.Write([]byte("  }\n"))
		d.PanicIfError(err)
	}
}
