// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/attic-labs/noms/go/d"
	humanize "github.com/dustin/go-humanize"
)

// Human Readable Serialization
type hrsWriter struct {
	ind        int
	w          io.Writer
	lineLength int
	err        error
}

func (w *hrsWriter) maybeWriteIndentation() {
	if w.lineLength == 0 {
		for i := 0; i < w.ind && w.err == nil; i++ {
			_, w.err = io.WriteString(w.w, "  ")
		}
		w.lineLength = 2 * w.ind
	}
}

func (w *hrsWriter) write(s string) {
	if w.err != nil {
		return
	}
	w.maybeWriteIndentation()
	var n int
	n, w.err = io.WriteString(w.w, s)
	w.lineLength += n
}

func (w *hrsWriter) indent() {
	w.ind++
}

func (w *hrsWriter) outdent() {
	w.ind--
}

func (w *hrsWriter) newLine() {
	w.write("\n")
	w.lineLength = 0
}

// hexWriter is used to write blob byte data as "00 01 ... 0f\n10 11 .."
// hexWriter is an io.Writer that writes to an underlying hrsWriter.
type hexWriter struct {
	hrs         *hrsWriter
	count       uint
	sizeWritten bool
	size        uint64
}

func (w *hexWriter) Write(p []byte) (n int, err error) {
	for _, v := range p {
		if w.count == 16 {
			if !w.sizeWritten {
				w.hrs.write("  // ")
				w.hrs.write(humanize.Bytes(w.size))
				w.sizeWritten = true
			}
			w.hrs.newLine()
			w.count = 0
		} else if w.count != 0 {
			w.hrs.write(" ")
		}
		if v < 0x10 {
			w.hrs.write("0")
		}
		w.hrs.write(strconv.FormatUint(uint64(v), 16))
		if w.hrs.err != nil {
			err = w.hrs.err
			return
		}
		n++
		w.count++
	}
	return
}

func (w *hrsWriter) Write(v Value) {
	switch v.Type().Kind() {
	case BoolKind:
		w.write(strconv.FormatBool(bool(v.(Bool))))
	case NumberKind:
		w.write(strconv.FormatFloat(float64(v.(Number)), 'g', -1, 64))

	case StringKind:
		w.write(strconv.Quote(string(v.(String))))

	case BlobKind:
		w.maybeWriteIndentation()
		blob := v.(Blob)
		encoder := &hexWriter{hrs: w, size: blob.Len()}
		_, w.err = io.Copy(encoder, blob.Reader())

	case ListKind:
		w.write("[")
		w.writeSize(v)
		w.indent()
		v.(List).Iter(func(v Value, i uint64) bool {
			if i == 0 {
				w.newLine()
			}
			w.Write(v)
			w.write(",")
			w.newLine()
			return w.err != nil
		})
		w.outdent()
		w.write("]")

	case MapKind:
		w.write("{")
		w.writeSize(v)
		w.indent()
		first := true
		v.(Map).Iter(func(key, val Value) bool {
			if first {
				w.newLine()
				first = false
			}
			w.Write(key)
			w.write(": ")
			w.Write(val)
			w.write(",")
			w.newLine()
			return w.err != nil
		})
		w.outdent()
		w.write("}")

	case RefKind:
		w.write(v.(Ref).TargetHash().String())

	case SetKind:
		w.write("{")
		w.writeSize(v)
		w.indent()
		first := true
		v.(Set).Iter(func(v Value) bool {
			if first {
				w.newLine()
				first = false
			}
			w.Write(v)
			w.write(",")
			w.newLine()
			return w.err != nil
		})
		w.outdent()
		w.write("}")

	case TypeKind:
		w.writeType(v.(*Type), nil)

	case StructKind:
		w.writeStruct(v.(Struct), true)

	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeStruct(v Struct, printStructName bool) {
	t := v.Type()

	desc := t.Desc.(StructDesc)
	if printStructName {
		w.write(t.Name())
		w.write(" ")
	}
	w.write("{")
	w.indent()

	first := true
	desc.IterFields(func(name string, t *Type) {
		fv := v.Get(name)
		if first {
			w.newLine()
			first = false
		}
		w.write(name)
		w.write(": ")
		w.Write(fv)
		w.write(",")
		w.newLine()
	})

	w.outdent()
	w.write("}")
}

func (w *hrsWriter) WriteTagged(v Value) {
	t := v.Type()
	switch t.Kind() {
	case BoolKind, NumberKind, StringKind:
		w.Write(v)
	case BlobKind, ListKind, MapKind, RefKind, SetKind, TypeKind, CycleKind:
		w.writeType(t, nil)
		w.write("(")
		w.Write(v)
		w.write(")")
	case StructKind:
		w.writeType(t, nil)
		w.write("(")
		w.writeStruct(v.(Struct), false)
		w.write(")")
	case ValueKind:
	default:
		panic("unreachable")
	}
}

type lenable interface {
	Len() uint64
}

func (w *hrsWriter) writeSize(v Value) {
	t := v.Type()
	switch t.Kind() {
	case ListKind, MapKind, SetKind:
		l := v.(lenable).Len()
		if l < 4 {
			return
		}
		w.write(fmt.Sprintf("  // %s items", humanize.Comma(int64(l))))
	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeType(t *Type, parentStructTypes []*Type) {
	switch t.Kind() {
	case BlobKind, BoolKind, NumberKind, StringKind, TypeKind, ValueKind:
		w.write(KindToString[t.Kind()])
	case ListKind, RefKind, SetKind, MapKind:
		w.write(KindToString[t.Kind()])
		w.write("<")
		for i, et := range t.Desc.(CompoundDesc).ElemTypes {
			if i != 0 {
				w.write(", ")
			}
			w.writeType(et, parentStructTypes)
			if w.err != nil {
				break
			}
		}
		w.write(">")
	case UnionKind:
		for i, et := range t.Desc.(CompoundDesc).ElemTypes {
			if i != 0 {
				w.write(" | ")
			}
			w.writeType(et, parentStructTypes)
			if w.err != nil {
				break
			}
		}
	case StructKind:
		w.writeStructType(t, parentStructTypes)
	case CycleKind:
		// This can happen for types which have unresolved cyclic refs
		w.write(fmt.Sprintf("Cycle<%d>", int(t.Desc.(CycleDesc))))
	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeStructType(t *Type, parentStructTypes []*Type) {
	idx, found := indexOfType(t, parentStructTypes)
	if found {
		w.writeCycle(uint8(uint32(len(parentStructTypes)) - 1 - idx))
		return
	}
	parentStructTypes = append(parentStructTypes, t)

	w.write("struct ")
	w.write(t.Name())
	w.write(" {")
	w.indent()
	desc := t.Desc.(StructDesc)
	first := true
	desc.IterFields(func(name string, t *Type) {
		if first {
			first = false
			w.newLine()
		}
		w.write(name)
		w.write(": ")
		w.writeType(t, parentStructTypes)
		w.write(",")
		w.newLine()
	})
	w.outdent()
	w.write("}")
}

func (w *hrsWriter) writeCycle(i uint8) {
	if w.err != nil {
		return
	}
	_, w.err = fmt.Fprintf(w.w, "Cycle<%d>", i)
}

func EncodedValue(v Value) string {
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf}
	w.Write(v)
	d.Chk.NoError(w.err)
	return buf.String()
}

// WriteEncodedValue writes the serialization of a value
func WriteEncodedValue(w io.Writer, v Value) error {
	hrs := &hrsWriter{w: w}
	hrs.Write(v)
	return hrs.err
}

func EncodedValueWithTags(v Value) string {
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf}
	w.WriteTagged(v)
	d.Chk.NoError(w.err)
	return buf.String()
}

// WriteEncodedValueWithTags writes the serialization of a value prefixed by its type.
func WriteEncodedValueWithTags(w io.Writer, v Value) error {
	hrs := &hrsWriter{w: w}
	hrs.WriteTagged(v)
	return hrs.err
}
