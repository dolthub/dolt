package types

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

// ReadValue reads and decodes a value from a chunk source. It is not considered an error for the requested chunk to be absent from cs; in this case, the function simply returns nil, nil.
func ReadValue(r ref.Ref, cs chunks.ChunkSource) Value {
	d.Chk.NotNil(cs)
	c := cs.Get(r)
	if c.IsEmpty() {
		return nil
	}

	v := enc.Decode(bytes.NewReader(c.Data()))

	if v, ok := v.(typedValueWrapper); ok {
		tv := fromTypedEncodeable(v, cs)
		// TODO: Figure out a better way to achieve this. BUG 490
		if tv, ok := tv.(compoundBlobStruct); ok {
			return convertToCompoundBlob(tv, cs)
		}
		return tv
	}

	return fromEncodeable(v, cs).Deref(cs)
}

func fromEncodeable(i interface{}, cs chunks.ChunkSource) Future {
	switch i := i.(type) {
	case bool:
		return futureFromValue(Bool(i))
	case int8:
		return futureFromValue(Int8(i))
	case int16:
		return futureFromValue(Int16(i))
	case int32:
		return futureFromValue(Int32(i))
	case int64:
		return futureFromValue(Int64(i))
	case float32:
		return futureFromValue(Float32(i))
	case float64:
		return futureFromValue(Float64(i))
	case uint8:
		return futureFromValue(UInt8(i))
	case uint16:
		return futureFromValue(UInt16(i))
	case uint32:
		return futureFromValue(UInt32(i))
	case uint64:
		return futureFromValue(UInt64(i))
	case string:
		return futureFromValue(NewString(i))
	case ref.Ref:
		return futureFromRef(i)
	case io.Reader:
		data, err := ioutil.ReadAll(i)
		d.Chk.NoError(err)
		return futureFromValue(newBlobLeaf(data))
	case []interface{}:
		return futureListFromIterable(i, cs)
	case enc.Map:
		return futureMapFromIterable(i, cs)
	case enc.Set:
		return futureSetFromIterable(i, cs)
	case enc.TypeRef:
		kind := NomsKind(i.Kind)
		desc := typeDescFromInterface(kind, i.Desc, cs)
		if desc, ok := desc.(UnresolvedDesc); ok {
			d.Chk.Equal(UnresolvedKind, kind)
			return futureFromValue(MakeTypeRef(desc.pkgRef, desc.ordinal))
		}
		return futureFromValue(buildType(i.Name, desc))
	case enc.CompoundBlob:
		blobs := make([]Future, len(i.Blobs))
		for idx, blobRef := range i.Blobs {
			blobs[idx] = fromEncodeable(blobRef, cs)
		}
		cb := newCompoundBlob(i.Offsets, blobs, cs)
		return futureFromValue(cb)
	case enc.Package:
		types := make([]TypeRef, len(i.Types))
		for idx, t := range i.Types {
			types[idx] = fromEncodeable(t, cs).Deref(cs).(TypeRef)
		}
		return futureFromValue(Package{types, i.Dependencies, &ref.Ref{}})
	default:
		d.Exp.Fail("Unknown encodeable", "%+v", i)
	}

	return nil
}

func futureListFromIterable(items []interface{}, cs chunks.ChunkSource) Future {
	output := futuresFromIterable(items, cs)
	return futureFromValue(listFromFutures(output, cs))
}

func futureMapFromIterable(items []interface{}, cs chunks.ChunkSource) Future {
	output := futuresFromIterable(items, cs)
	return futureFromValue(mapFromFutures(output, cs))
}

func futureSetFromIterable(items []interface{}, cs chunks.ChunkSource) Future {
	output := futuresFromIterable(items, cs)
	return futureFromValue(setFromFutures(output, cs))
}

func typeDescFromInterface(kind NomsKind, i interface{}, cs chunks.ChunkSource) TypeDesc {
	switch kind {
	case ListKind, RefKind, SetKind:
		return CompoundDesc{kind, []TypeRef{fromEncodeable(i, cs).Deref(cs).(TypeRef)}}
	case MapKind:
		items := i.([]interface{})
		d.Chk.Len(items, 2)
		elemTypes := []TypeRef{
			fromEncodeable(items[0], cs).Deref(cs).(TypeRef),
			fromEncodeable(items[1], cs).Deref(cs).(TypeRef),
		}
		return CompoundDesc{kind, elemTypes}
	case EnumKind:
		items := i.([]interface{})
		ids := make([]string, len(items))
		for idx, item := range items {
			ids[idx] = item.(string)
		}
		return EnumDesc{ids}
	case StructKind:
		items := i.(enc.Map)
		return StructDescFromMap(mapFromFutures(futuresFromIterable(items, cs), cs))
	case UnresolvedKind:
		items := i.([]interface{})
		pkgRef := items[0].(ref.Ref)
		ordinal := items[1].(int16)
		return UnresolvedDesc{pkgRef, ordinal}
	default:
		if IsPrimitiveKind(kind) {
			d.Chk.Nil(i, "Primitive TypeRefs have no serialized description.")
			return PrimitiveDesc(kind)
		}
		d.Exp.Fail("Unrecognized Kind:", "%v", kind)
		panic("unreachable")
	}
}

func futuresFromIterable(items []interface{}, cs chunks.ChunkSource) (f []Future) {
	f = make([]Future, len(items))
	for i, inVal := range items {
		f[i] = fromEncodeable(inVal, cs)
	}
	return
}

func convertToCompoundBlob(cbs compoundBlobStruct, cs chunks.ChunkSource) compoundBlob {
	offsets := cbs.Offsets().Def()
	refs := cbs.Blobs().Def()
	futures := make([]Future, len(refs))
	for i, r := range refs {
		futures[i] = futureFromRef(r)
	}
	return newCompoundBlob(offsets, futures, cs)
}
