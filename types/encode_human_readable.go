package types

import (
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
	case Uint8Kind:
		w.write(strconv.FormatUint(uint64(v.(Uint8)), 10))
	case Uint16Kind:
		w.write(strconv.FormatUint(uint64(v.(Uint16)), 10))
	case Uint32Kind:
		w.write(strconv.FormatUint(uint64(v.(Uint32)), 10))
	case Uint64Kind:
		w.write(strconv.FormatUint(uint64(v.(Uint64)), 10))
	case Int8Kind:
		w.write(strconv.FormatInt(int64(v.(Int8)), 10))
	case Int16Kind:
		w.write(strconv.FormatInt(int64(v.(Int16)), 10))
	case Int32Kind:
		w.write(strconv.FormatInt(int64(v.(Int32)), 10))
	case Int64Kind:
		w.write(strconv.FormatInt(int64(v.(Int64)), 10))
	case Float32Kind:
		w.write(strconv.FormatFloat(float64(v.(Float32)), 'g', -1, 32))
	case Float64Kind:
		w.write(strconv.FormatFloat(float64(v.(Float64)), 'g', -1, 64))

	case StringKind:
		w.write(strconv.Quote(v.(String).String()))

	case BlobKind:
		w.maybeWriteIndentation()
		blob := v.(Blob)
		encoder := base64.NewEncoder(base64.RawStdEncoding, w.w)
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
		w.write(v.(RefBase).TargetRef().String())

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
		w.writeTypeAsValue(v.(Type))

	case UnresolvedKind:
		w.writeUnresolved(v, true)

	case PackageKind:
		panic("not implemented")

	case ValueKind, EnumKind, StructKind:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeUnresolved(v Value, printStructName bool) {
	t := v.Type()
	pkg := LookupPackage(t.PackageRef())
	typeDef := pkg.Types()[t.Ordinal()]
	switch typeDef.Kind() {
	case StructKind:
		v := v.(Struct)
		desc := typeDef.Desc.(StructDesc)
		if printStructName {
			w.write(typeDef.Name())
			w.write(" ")
		}
		w.write("{")
		w.indent()

		writeField := func(f Field, v Value, i int) {
			if i == 0 {
				w.newLine()
			}
			w.write(f.Name)
			w.write(": ")
			w.Write(v)
			w.write(",")
			w.newLine()
		}

		for i, f := range desc.Fields {
			if fv, present := v.MaybeGet(f.Name); present {
				writeField(f, fv, i)
			}
		}
		if len(desc.Union) > 0 {
			f := desc.Union[v.UnionIndex()]
			fv := v.UnionValue()
			writeField(f, fv, 0)
		}

		w.outdent()
		w.write("}")

	case EnumKind:
		v := v.(Enum)
		i := enumPrimitiveValueFromType(v, t)
		w.write(typeDef.Desc.(EnumDesc).IDs[i])

	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) WriteTagged(v Value) {
	t := v.Type()
	switch t.Kind() {
	case BoolKind, StringKind:
		w.Write(v)
	case Uint8Kind, Uint16Kind, Uint32Kind, Uint64Kind, Int8Kind, Int16Kind, Int32Kind, Int64Kind, Float32Kind, Float64Kind, BlobKind, ListKind, MapKind, RefKind, SetKind, TypeKind:
		w.writeTypeAsValue(t)
		w.write("(")
		w.Write(v)
		w.write(")")

	case UnresolvedKind:
		w.writeTypeAsValue(t)
		w.write("(")
		w.writeUnresolved(v, false)
		w.write(")")
	case PackageKind:
		panic("not implemented")

	case ValueKind, EnumKind, StructKind:
	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeTypeAsValue(t Type) {
	switch t.Kind() {
	case BlobKind, BoolKind, Float32Kind, Float64Kind, Int16Kind, Int32Kind, Int64Kind, Int8Kind, StringKind, TypeKind, Uint16Kind, Uint32Kind, Uint64Kind, Uint8Kind, ValueKind:
		w.write(KindToString[t.Kind()])
	case ListKind, RefKind, SetKind:
		w.write(KindToString[t.Kind()])
		w.write("<")
		w.writeTypeAsValue(t.Desc.(CompoundDesc).ElemTypes[0])
		w.write(">")
	case MapKind:
		w.write(KindToString[t.Kind()])
		w.write("<")
		w.writeTypeAsValue(t.Desc.(CompoundDesc).ElemTypes[0])
		w.write(", ")
		w.writeTypeAsValue(t.Desc.(CompoundDesc).ElemTypes[1])
		w.write(">")
	case EnumKind:
		w.write("enum ")
		w.write(t.Name())
		w.write(" {")
		w.indent()
		for i, id := range t.Desc.(EnumDesc).IDs {
			if i == 0 {
				w.newLine()
			}
			w.write(id)
			w.newLine()
		}
		w.outdent()
		w.write("}")
	case StructKind:
		w.write("struct ")
		w.write(t.Name())
		w.write(" {")
		w.indent()
		desc := t.Desc.(StructDesc)
		writeField := func(f Field, i int) {
			if i == 0 {
				w.newLine()
			}
			w.write(f.Name)
			w.write(": ")
			if f.Optional {
				w.write("optional ")
			}
			w.writeTypeAsValue(f.T)
			w.newLine()
		}
		for i, f := range desc.Fields {
			writeField(f, i)
		}
		if len(desc.Union) > 0 {
			w.write("union {")
			w.indent()
			for i, f := range desc.Union {
				writeField(f, i)
			}
			w.outdent()
			w.write("}")
			w.newLine()
		}
		w.outdent()
		w.write("}")
	case UnresolvedKind:
		w.writeUnresolvedTypeRef(t, true)
	case PackageKind:
		panic("not implemented")
	}
}

func (w *hrsWriter) writeUnresolvedTypeRef(t Type, printStructName bool) {
	pkg := LookupPackage(t.PackageRef())
	typeDef := pkg.Types()[t.Ordinal()]
	switch typeDef.Kind() {
	case StructKind:
		w.write("Struct")
	case EnumKind:
		w.write("Enum")
	default:
		panic("unreachable")
	}
	fmt.Fprintf(w.w, "<%s, %s, %d>", typeDef.Name(), t.PackageRef(), t.Ordinal())
}
