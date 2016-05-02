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
}

func (w *hrsWriter) maybeWriteIndentation() {
	if w.lineLength == 0 {
		for i := 0; i < w.ind; i++ {
			io.WriteString(w.w, "  ")
		}
		w.lineLength = 2 * w.ind
	}
}

func (w *hrsWriter) write(s string) {
	w.maybeWriteIndentation()
	n, err := io.WriteString(w.w, s)
	w.lineLength += len(s)
	d.Chk.NoError(err)
	d.Chk.Equal(len(s), n)
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
		_, err := io.Copy(encoder, blob.Reader())
		d.Chk.NoError(err)
		encoder.Close()

	case ListKind:
		w.write("[")
		w.indent()
		v.(List).IterAll(func(v Value, i uint64) {
			if i == 0 {
				w.newLine()
			}
			w.Write(v)
			w.write(",")
			w.newLine()
		})
		w.outdent()
		w.write("]")

	case MapKind:
		w.write("{")
		w.indent()
		i := 0
		v.(Map).IterAll(func(key, val Value) {
			if i == 0 {
				w.newLine()
			}
			w.Write(key)
			w.write(": ")
			w.Write(val)
			w.write(",")
			w.newLine()
			i++
		})
		w.outdent()
		w.write("}")

	case RefKind:
		w.write(v.(Ref).TargetRef().String())

	case SetKind:
		w.write("{")
		w.indent()
		i := 0
		v.(Set).IterAll(func(v Value) {
			if i == 0 {
				w.newLine()
			}
			w.Write(v)
			w.write(",")
			w.newLine()
			i++
		})
		w.outdent()
		w.write("}")

	case TypeKind:
		w.writeType(v.(*Type), nil)

	case StructKind:
		w.writeStruct(v.(Struct), true)

	default:
	case ValueKind, ParentKind:
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

	for i, f := range desc.Fields {
		if fv, present := v.MaybeGet(f.Name); present {
			if i == 0 {
				w.newLine()
			}
			w.write(f.Name)
			w.write(": ")
			w.Write(fv)
			w.write(",")
			w.newLine()
		}
	}

	w.outdent()
	w.write("}")
}

func (w *hrsWriter) WriteTagged(v Value) {
	t := v.Type()
	switch t.Kind() {
	case BoolKind, StringKind:
		w.Write(v)
	case NumberKind, BlobKind, ListKind, MapKind, RefKind, SetKind, TypeKind, ParentKind:
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
	case ListKind, RefKind, SetKind:
		w.write(KindToString[t.Kind()])
		w.write("<")
		w.writeType(t.Desc.(CompoundDesc).ElemTypes[0], parentStructTypes)
		w.write(">")
	case MapKind:
		w.write(KindToString[t.Kind()])
		w.write("<")
		w.writeType(t.Desc.(CompoundDesc).ElemTypes[0], parentStructTypes)
		w.write(", ")
		w.writeType(t.Desc.(CompoundDesc).ElemTypes[1], parentStructTypes)
		w.write(">")
	case StructKind:
		w.writeStructType(t, parentStructTypes)
	case ParentKind:
	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeStructType(t *Type, parentStructTypes []*Type) {
	idx := indexOfType(t, parentStructTypes)
	if idx != -1 {
		w.writeParent(uint8(len(parentStructTypes) - 1 - idx))
		return
	}
	parentStructTypes = append(parentStructTypes, t)

	w.write("struct ")
	w.write(t.Name())
	w.write(" {")
	w.indent()
	desc := t.Desc.(StructDesc)
	for i, f := range desc.Fields {
		if i == 0 {
			w.newLine()
		}
		w.write(f.Name)
		w.write(": ")
		w.writeType(f.Type, parentStructTypes)
		w.newLine()
	}
	w.outdent()
	w.write("}")
}

func (w *hrsWriter) writeParent(i uint8) {
	fmt.Fprintf(w.w, "Parent<%d>", i)
}

func EncodedValue(v Value) string {
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf}
	w.Write(v)
	return buf.String()
}

func EncodedValueWithTags(v Value) string {
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf}
	w.WriteTagged(v)
	return buf.String()
}
