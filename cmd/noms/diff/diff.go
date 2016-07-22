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

func shouldDescend(v1, v2 types.Value) bool {
	kind := v1.Type().Kind()
	return !types.IsPrimitiveKind(kind) && kind == v2.Type().Kind()
}

func Diff(w io.Writer, v1, v2 types.Value) error {
	return d.Try(func() {
		diff(w, types.NewPath().AddField("/"), nil, v1, v2)
	})
}

func diff(w io.Writer, p types.Path, key, v1, v2 types.Value) {
	if v1 == nil && v2 != nil {
		line(w, addPrefix, key, v2)
	} else if v1 != nil && v2 == nil {
		line(w, subPrefix, key, v1)
	} else if !v1.Equals(v2) {
		if shouldDescend(v1, v2) {
			switch v1.Type().Kind() {
			case types.ListKind:
				diffLists(w, p, v1.(types.List), v2.(types.List))
			case types.MapKind:
				diffMaps(w, p, v1.(types.Map), v2.(types.Map))
			case types.SetKind:
				diffSets(w, p, v1.(types.Set), v2.(types.Set))
			case types.StructKind:
				diffStructs(w, p, v1.(types.Struct), v2.(types.Struct))
			default:
				panic("Unrecognized type in diff function")
			}
		} else {
			line(w, subPrefix, key, v1)
			line(w, addPrefix, key, v2)
		}
	}
}

func diffLists(w io.Writer, p types.Path, v1, v2 types.List) {
	spliceChan := make(chan types.Splice)
	closeChan := make(chan struct{})
	doneChan := make(chan struct{})

	go func() {
		v2.Diff(v1, spliceChan, closeChan)
		close(spliceChan)
		doneChan <- struct{}{}
	}()
	defer waitForCloseOrDone(closeChan, doneChan) // see comment for explanation

	wroteHeader := false

	for splice := range spliceChan {
		if splice.SpRemoved == splice.SpAdded {
			for i := uint64(0); i < splice.SpRemoved; i++ {
				lastEl := v1.Get(splice.SpAt + i)
				newEl := v2.Get(splice.SpFrom + i)
				if shouldDescend(lastEl, newEl) {
					idx := types.Number(splice.SpAt + i)
					diff(w, p.AddIndex(idx), idx, lastEl, newEl)
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

func diffMaps(w io.Writer, p types.Path, v1, v2 types.Map) {
	changeChan := make(chan types.ValueChanged)
	closeChan := make(chan struct{})
	doneChan := make(chan struct{})

	go func() {
		v2.Diff(v1, changeChan, closeChan)
		close(changeChan)
		doneChan <- struct{}{}
	}()
	defer waitForCloseOrDone(closeChan, doneChan) // see comment for explanation

	wroteHeader := false

	for change := range changeChan {
		switch change.ChangeType {
		case types.DiffChangeAdded:
			line(w, addPrefix, change.V, v2.Get(change.V))
		case types.DiffChangeRemoved:
			line(w, subPrefix, change.V, v1.Get(change.V))
		case types.DiffChangeModified:
			c1, c2 := v1.Get(change.V), v2.Get(change.V)
			if shouldDescend(c1, c2) {
				buf := &bytes.Buffer{}
				writeEncodedValueWithTags(buf, change.V)
				diff(w, p.AddField(buf.String()), change.V, c1, c2)
			} else {
				wroteHeader = writeHeader(w, wroteHeader, p)
				line(w, subPrefix, change.V, c1)
				line(w, addPrefix, change.V, c2)
			}
		default:
			panic("unknown change type")
		}
	}

	writeFooter(w, wroteHeader)
}

func diffStructs(w io.Writer, p types.Path, v1, v2 types.Struct) {
	wroteHeader := false

	for _, field := range types.StructDiff(v1, v2) {
		f1 := v1.Get(field)
		f2 := v2.Get(field)
		if shouldDescend(f1, f2) {
			diff(w, p.AddField(field), types.String(field), f1, f2)
		} else {
			wroteHeader = writeHeader(w, wroteHeader, p)
			line(w, subPrefix, types.String(field), f1)
			line(w, addPrefix, types.String(field), f2)
		}
	}

	writeFooter(w, wroteHeader)
}

func diffSets(w io.Writer, p types.Path, v1, v2 types.Set) {
	changeChan := make(chan types.ValueChanged)
	closeChan := make(chan struct{})
	doneChan := make(chan struct{})

	go func() {
		v2.Diff(v1, changeChan, closeChan)
		close(changeChan)
		doneChan <- struct{}{}
	}()
	defer waitForCloseOrDone(closeChan, doneChan) // see comment for explanation

	wroteHeader := false

	for change := range changeChan {
		wroteHeader = writeHeader(w, wroteHeader, p)
		switch change.ChangeType {
		case types.DiffChangeAdded:
			line(w, addPrefix, nil, change.V)
		case types.DiffChangeRemoved:
			line(w, subPrefix, nil, change.V)
		default:
			panic("unknown change type")
		}
	}

	writeFooter(w, wroteHeader)
}

type prefixWriter struct {
	w      io.Writer
	prefix []byte
}

// TODO: Not sure if we want to use a writer to do this for the longterm but, if so, we can
// probably do better than writing byte by byte
func (pw prefixWriter) Write(bytes []byte) (int, error) {
	for i, b := range bytes {
		_, err := pw.w.Write([]byte{b})
		if err == nil && b == '\n' {
			_, err = pw.w.Write(pw.prefix)
		}
		if err != nil {
			return i, err
		}
	}
	return len(bytes), nil
}

func line(w io.Writer, startStr string, key, val types.Value) {
	start := []byte(startStr)
	pw := prefixWriter{w, []byte(start)}

	write(w, start)
	if key != nil {
		writeEncodedValue(pw, key)
		write(w, []byte(": "))
	}
	writeEncodedValue(pw, val)
	write(w, []byte("\n"))
}

func writeHeader(w io.Writer, wroteHeader bool, p types.Path) bool {
	if !wroteHeader {
		write(w, []byte(p.String()))
		write(w, []byte(" {\n"))
	}
	return true
}

func writeFooter(w io.Writer, wroteHeader bool) {
	if wroteHeader {
		write(w, []byte("  }\n"))
	}
}

func write(w io.Writer, b []byte) {
	_, err := w.Write(b)
	d.PanicIfError(err)
}

func writeEncodedValue(w io.Writer, v types.Value) {
	d.PanicIfError(types.WriteEncodedValue(w, v))
}

func writeEncodedValueWithTags(w io.Writer, v types.Value) {
	d.PanicIfError(types.WriteEncodedValueWithTags(w, v))
}

// This is intended to be used
// - when called as deferred,
// - with a separate goroutine running a Diff, cancelable by |closeChan|, which writes to |doneChan| when it's finished.
// I.e.
//
// go func() {
//   Diff(...)
//   doneChan <- struct{}{}
// }()
// defer waitForCloseOrDone()
//
// It's designed to handle 2 cases: (1) the outer function panic'd so Diff didn't finish, or (2) the Diff finished and the outer function exited normally.
// If (1) we try to cancel the diff by sending to |closeChan|.
// If (2) we wait for the Diff to finish by blocking on |doneChan|.
// In both cases this deferred function will be unblocked.
func waitForCloseOrDone(closeChan, doneChan chan struct{}) {
	select {
	case closeChan <- struct{}{}:
		<-doneChan // after cancelling, Diff will exit then block on |doneChan|, so unblock it
	case <-doneChan:
	}
}
