// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"

	"github.com/attic-labs/noms/d"
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

func (w *hrsWriter) Write(v Value) {
	switch v.Type().Kind() {
	case BoolKind:
		w.write(strconv.FormatBool(bool(v.(Bool))))
	case NumberKind:
		w.write(strconv.FormatFloat(float64(v.(Number)), 'g', -1, 64))

	case StringKind:
		w.write(strconv.Quote(v.(String).String()))

	case BlobKind:
		w.maybeWriteIndentation()
		blob := v.(Blob)
		// TODO: Use RawStdEncoding
		encoder := base64.NewEncoder(base64.StdEncoding, w.w)
		defer encoder.Close()
		_, w.err = io.Copy(encoder, blob.Reader())

	case ListKind:
		w.write("[")
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
	case BoolKind, StringKind:
		w.Write(v)
	case NumberKind, BlobKind, ListKind, MapKind, RefKind, SetKind, TypeKind, CycleKind:
		// TODO: Numbers have unique syntax now...
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
	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeStructType(t *Type, parentStructTypes []*Type) {
	idx := indexOfType(t, parentStructTypes)
	if idx != -1 {
		w.writeCycle(uint8(len(parentStructTypes) - 1 - idx))
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
