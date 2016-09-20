// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"io"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/writers"
	humanize "github.com/dustin/go-humanize"
)

type prefixOp string

const (
	ADD = "+   "
	DEL = "-   "
)

type (
	diffFunc     func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{})
	lineFunc     func(w io.Writer, op prefixOp, key, val types.Value) error
	pathPartFunc func(v types.Value) types.PathPart
	valueFunc    func(k types.Value) types.Value
)

type diffWriter struct {
	w         io.Writer
	leftRight bool
}

func (w diffWriter) Write(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func shouldDescend(v1, v2 types.Value) bool {
	kind := v1.Type().Kind()
	return !types.IsPrimitiveKind(kind) && kind == v2.Type().Kind() && kind != types.RefKind
}

// Diff writes the diff from |v1| to |v2| to |w|.
// If |leftRight| is true then the left-right diff is used for ordered sequences - see Diff vs DiffLeftRight in Set and Map.
func Diff(w io.Writer, v1, v2 types.Value, leftRight bool) error {
	if v1.Equals(v2) {
		return nil
	}

	dw := diffWriter{w, leftRight}

	if !shouldDescend(v1, v2) {
		line(dw, DEL, nil, v1)
		return line(dw, ADD, nil, v2)
	}

	return diff(dw, types.Path{}, nil, v1, v2)
}

func diff(w diffWriter, p types.Path, key, v1, v2 types.Value) error {
	switch v1.Type().Kind() {
	case types.ListKind:
		return diffLists(w, p, v1.(types.List), v2.(types.List))
	case types.MapKind:
		return diffMaps(w, p, v1.(types.Map), v2.(types.Map))
	case types.SetKind:
		return diffSets(w, p, v1.(types.Set), v2.(types.Set))
	case types.StructKind:
		return diffStructs(w, p, v1.(types.Struct), v2.(types.Struct))
	default:
		panic("Unrecognized type in diff function")
	}
}

func diffLists(w diffWriter, p types.Path, v1, v2 types.List) (err error) {
	spliceChan := make(chan types.Splice)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		v2.Diff(v1, spliceChan, stopChan)
		close(spliceChan)
	}()

	wroteHdr := false

	for splice := range spliceChan {
		if err != nil {
			break
		}

		if splice.SpRemoved == splice.SpAdded {
			// Heuristic: list only has modifications.
			for i := uint64(0); i < splice.SpRemoved && err == nil; i++ {
				lastEl := v1.Get(splice.SpAt + i)
				newEl := v2.Get(splice.SpFrom + i)
				if shouldDescend(lastEl, newEl) {
					idx := types.Number(splice.SpAt + i)
					writeFooter(w, &wroteHdr)
					err = diff(w, append(p, types.NewIndexPath(idx)), idx, lastEl, newEl)
				} else {
					writeHeader(w, p, &wroteHdr)
					line(w, DEL, nil, v1.Get(splice.SpAt+i))
					err = line(w, ADD, nil, v2.Get(splice.SpFrom+i))
				}
			}
			continue
		}

		// Heuristic: list only has additions/removals.
		for i := uint64(0); i < splice.SpRemoved && err == nil; i++ {
			writeHeader(w, p, &wroteHdr)
			err = line(w, DEL, nil, v1.Get(splice.SpAt+i))
		}
		for i := uint64(0); i < splice.SpAdded && err == nil; i++ {
			writeHeader(w, p, &wroteHdr)
			err = line(w, ADD, nil, v2.Get(splice.SpFrom+i))
		}
	}

	err = writeFooter(w, &wroteHdr)

	if err != nil {
		stopChan <- struct{}{}
		// Wait for diff to stop.
		for range spliceChan {
		}
	}
	return
}

func diffMaps(w diffWriter, p types.Path, v1, v2 types.Map) error {
	return diffOrdered(w, p, line,
		func(v types.Value) types.PathPart { return types.NewIndexPath(v) },
		func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
			if w.leftRight {
				v2.DiffLeftRight(v1, cc, sc)
			} else {
				v2.Diff(v1, cc, sc)
			}
		},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return v1.Get(k) },
		func(k types.Value) types.Value { return v2.Get(k) },
	)
}

func diffStructs(w diffWriter, p types.Path, v1, v2 types.Struct) error {
	str := func(v types.Value) string {
		return string(v.(types.String))
	}
	return diffOrdered(w, p, field,
		func(v types.Value) types.PathPart { return types.NewFieldPath(str(v)) },
		func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
			v2.Diff(v1, cc, sc)
		},
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return v1.Get(str(k)) },
		func(k types.Value) types.Value { return v2.Get(str(k)) },
	)
}

func diffSets(w diffWriter, p types.Path, v1, v2 types.Set) error {
	return diffOrdered(w, p, line,
		func(v types.Value) types.PathPart { return types.NewIndexPath(v) },
		func(cc chan<- types.ValueChanged, sc <-chan struct{}) {
			if w.leftRight {
				v2.DiffLeftRight(v1, cc, sc)
			} else {
				v2.Diff(v1, cc, sc)
			}
		},
		func(k types.Value) types.Value { return nil },
		func(k types.Value) types.Value { return k },
		func(k types.Value) types.Value { return k },
	)
}

func diffOrdered(w diffWriter, p types.Path, lf lineFunc, ppf pathPartFunc, df diffFunc, kf, v1, v2 valueFunc) (err error) {
	changeChan := make(chan types.ValueChanged)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		df(changeChan, stopChan)
		close(changeChan)
	}()

	wroteHdr := false

	for change := range changeChan {
		if err != nil {
			break
		}

		k := kf(change.V)

		switch change.ChangeType {
		case types.DiffChangeAdded:
			writeHeader(w, p, &wroteHdr)
			err = lf(w, ADD, k, v2(change.V))
		case types.DiffChangeRemoved:
			writeHeader(w, p, &wroteHdr)
			err = lf(w, DEL, k, v1(change.V))
		case types.DiffChangeModified:
			c1, c2 := v1(change.V), v2(change.V)
			if shouldDescend(c1, c2) {
				writeFooter(w, &wroteHdr)
				err = diff(w, append(p, ppf(k)), change.V, c1, c2)
			} else {
				writeHeader(w, p, &wroteHdr)
				lf(w, DEL, k, c1)
				err = lf(w, ADD, k, c2)
			}
		default:
			panic("unknown change type")
		}
	}

	writeFooter(w, &wroteHdr)

	if err != nil {
		stopChan <- struct{}{}
		// Wait for diff to stop.
		for range changeChan {
		}
	}
	return
}

func writeHeader(w io.Writer, p types.Path, wroteHdr *bool) error {
	if *wroteHdr {
		return nil
	}
	*wroteHdr = true
	hdr := "(root)"
	if len(p) > 0 {
		hdr = p.String()
	}
	return write(w, []byte(hdr+" {\n"))
}

func writeFooter(w io.Writer, wroteHdr *bool) error {
	if !*wroteHdr {
		return nil
	}
	*wroteHdr = false
	return write(w, []byte("  }\n"))
}

func line(w io.Writer, op prefixOp, key, val types.Value) error {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(op)
	}
	pw := &writers.PrefixWriter{Dest: w, PrefixFunc: genPrefix, NeedsPrefix: true}
	if key != nil {
		writeEncodedValue(pw, key)
		write(w, []byte(": "))
	}
	writeEncodedValue(pw, val)
	return write(w, []byte("\n"))
}

func field(w io.Writer, op prefixOp, name, val types.Value) error {
	genPrefix := func(w *writers.PrefixWriter) []byte {
		return []byte(op)
	}
	pw := &writers.PrefixWriter{Dest: w, PrefixFunc: genPrefix, NeedsPrefix: true}
	write(pw, []byte(name.(types.String)))
	write(w, []byte(": "))
	writeEncodedValue(pw, val)
	return write(w, []byte("\n"))
}

func writeEncodedValue(w io.Writer, v types.Value) error {
	if v.Type().Kind() != types.BlobKind {
		return types.WriteEncodedValue(w, v)
	}
	write(w, []byte("Blob ("))
	write(w, []byte(humanize.Bytes(v.(types.Blob).Len())))
	return write(w, []byte(")"))
}

func write(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}
