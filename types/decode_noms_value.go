package types

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func fromTypedEncodeable(i []interface{}, vr ValueReader) Value {
	r := newJSONArrayReader(i, vr)
	return r.readValue()
}

type jsonArrayReader struct {
	a  []interface{}
	i  int
	vr ValueReader
}

func newJSONArrayReader(a []interface{}, vr ValueReader) *jsonArrayReader {
	return &jsonArrayReader{a: a, i: 0, vr: vr}
}

func (r *jsonArrayReader) read() interface{} {
	v := r.a[r.i]
	r.i++
	return v
}

func (r *jsonArrayReader) atEnd() bool {
	return r.i >= len(r.a)
}

func (r *jsonArrayReader) readString() string {
	return r.read().(string)
}

func (r *jsonArrayReader) readBool() bool {
	return r.read().(bool)
}

func (r *jsonArrayReader) readFloat() float64 {
	v, err := strconv.ParseFloat(r.readString(), 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readInt() int64 {
	v, err := strconv.ParseInt(r.readString(), 10, 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readUint() uint64 {
	v, err := strconv.ParseUint(r.readString(), 10, 64)
	d.Chk.Nil(err)
	return v
}

func (r *jsonArrayReader) readUint8() uint8 {
	return uint8(r.read().(float64))
}

func (r *jsonArrayReader) readUint16() uint16 {
	return uint16(r.read().(float64))
}

func (r *jsonArrayReader) readArray() []interface{} {
	return r.read().([]interface{})
}

func (r *jsonArrayReader) readKind() NomsKind {
	return NomsKind(r.read().(float64))
}

func (r *jsonArrayReader) readRef(t *Type) Ref {
	ref := ref.Parse(r.readString())
	height := r.readUint()
	return NewTypedRef(t, ref, height)
}

func (r *jsonArrayReader) readType(parentStructTypes []*Type) *Type {
	k := r.readKind()
	switch k {
	case ListKind:
		return MakeListType(r.readType(parentStructTypes))
	case MapKind:
		return MakeMapType(r.readType(parentStructTypes), r.readType(parentStructTypes))
	case RefKind:
		return MakeRefType(r.readType(parentStructTypes))
	case SetKind:
		return MakeSetType(r.readType(parentStructTypes))
	case StructKind:
		return r.readStructType(parentStructTypes)
	case UnionKind:
		l := r.readUint16()
		elemTypes := make([]*Type, l)
		for i := uint16(0); i < l; i++ {
			elemTypes[i] = r.readType(parentStructTypes)
		}
		return MakeUnionType(elemTypes...)
	case ParentKind:
		i := r.readUint8()
		d.Chk.True(i < uint8(len(parentStructTypes)))
		return parentStructTypes[len(parentStructTypes)-1-int(i)]
	}

	d.Chk.True(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *jsonArrayReader) readBlobLeafSequence() indexedSequence {
	s := r.readString()
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(s))
	b, err := ioutil.ReadAll(decoder)
	d.Exp.NoError(err)
	return newBlobLeafSequence(r.vr, b)
}

func (r *jsonArrayReader) readListLeafSequence(t *Type) indexedSequence {
	data := []Value{}
	for !r.atEnd() {
		v := r.readValue()
		data = append(data, v)
	}

	return newListLeafSequence(t, r.vr, data...)
}

func (r *jsonArrayReader) readSetLeafSequence(t *Type) orderedSequence {
	data := []Value{}
	for !r.atEnd() {
		v := r.readValue()
		data = append(data, v)
	}

	return newSetLeafSequence(t, r.vr, data...)
}

func (r *jsonArrayReader) readMapLeafSequence(t *Type) orderedSequence {
	data := []mapEntry{}
	for !r.atEnd() {
		k := r.readValue()
		v := r.readValue()
		data = append(data, mapEntry{k, v})
	}

	return newMapLeafSequence(t, r.vr, data...)
}

func (r *jsonArrayReader) readMetaSequence() metaSequenceData {
	data := metaSequenceData{}
	for !r.atEnd() {
		ref := r.readValue().(Ref)
		v := r.readValue()
		numLeaves := uint64(r.readUint())
		data = append(data, newMetaTuple(v, nil, ref, numLeaves))
	}

	return data
}

func (r *jsonArrayReader) readIndexedMetaSequence(t *Type) indexedMetaSequence {
	return newIndexedMetaSequence(r.readMetaSequence(), t, r.vr)
}

func (r *jsonArrayReader) readOrderedMetaSequence(t *Type) orderedMetaSequence {
	return newOrderedMetaSequence(r.readMetaSequence(), t, r.vr)
}

func (r *jsonArrayReader) readValue() Value {
	t := r.readType(nil)
	switch t.Kind() {
	case BlobKind:
		isMeta := r.readBool()
		if isMeta {
			r2 := newJSONArrayReader(r.readArray(), r.vr)
			return newBlob(r2.readIndexedMetaSequence(t))
		}

		return newBlob(r.readBlobLeafSequence())
	case BoolKind:
		return Bool(r.read().(bool))
	case NumberKind:
		return Number(r.readFloat())
	case StringKind:
		return NewString(r.readString())
	case ListKind:
		isMeta := r.readBool()
		r2 := newJSONArrayReader(r.readArray(), r.vr)
		if isMeta {
			return newList(r2.readIndexedMetaSequence(t))
		}
		return newList(r2.readListLeafSequence(t))
	case MapKind:
		isMeta := r.readBool()
		r2 := newJSONArrayReader(r.readArray(), r.vr)
		if isMeta {
			return newMap(r2.readOrderedMetaSequence(t))
		}
		return newMap(r2.readMapLeafSequence(t))
	case RefKind:
		return r.readRef(t)
	case SetKind:
		isMeta := r.readBool()
		r2 := newJSONArrayReader(r.readArray(), r.vr)
		if isMeta {
			return newSet(r2.readOrderedMetaSequence(t))
		}
		return newSet(r2.readSetLeafSequence(t))
	case StructKind:
		return r.readStruct(t)
	case TypeKind:
		return r.readType(nil)
	case ParentKind, UnionKind, ValueKind:
		d.Chk.Fail(fmt.Sprintf("A value instance can never have type %s", KindToString[t.Kind()]))
	}

	panic("not reachable")
}

func (r *jsonArrayReader) readStruct(t *Type) Value {
	// We've read `[StructKind, name, fields, unions` at this point
	values := []Value{}
	desc := t.Desc.(StructDesc)
	desc.IterFields(func(name string, t *Type) {
		values = append(values, r.readValue())
	})

	return structBuilder(values, t)
}

func (r *jsonArrayReader) readStructType(parentStructTypes []*Type) *Type {
	name := r.readString()

	fields := map[string]*Type{}
	st := MakeStructType(name, fields)
	parentStructTypes = append(parentStructTypes, st)
	desc := st.Desc.(StructDesc)

	fieldReader := newJSONArrayReader(r.readArray(), r.vr)
	for !fieldReader.atEnd() {
		fieldName := fieldReader.readString()
		fieldType := fieldReader.readType(parentStructTypes)
		fields[fieldName] = fieldType
	}
	desc.Fields = fields
	st.Desc = desc
	return st
}
