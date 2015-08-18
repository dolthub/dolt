package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

type primitive interface {
	ToPrimitive() interface{}
}

func WriteValue(v Value, cs chunks.ChunkSink) ref.Ref {
	d.Chk.NotNil(cs)

	e := toEncodeable(v, cs)
	dst := cs.Put()
	enc.Encode(dst, e)
	return dst.Ref()
}

func toEncodeable(v Value, cs chunks.ChunkSink) interface{} {
	switch v := v.(type) {
	case blobLeaf:
		return v.Reader()
	case compoundBlob:
		return encCompoundBlobFromCompoundBlob(v, cs)
	case List:
		return makeListEncodeable(v, cs)
	case Map:
		return makeMapEncodeable(v, cs)
	case primitive:
		return v.ToPrimitive()
	case Ref:
		return v.Ref()
	case Set:
		return makeSetEncodeable(v, cs)
	case String:
		return v.String()
	default:
		return v
	}
}

func encCompoundBlobFromCompoundBlob(cb compoundBlob, cs chunks.ChunkSink) interface{} {
	refs := make([]ref.Ref, len(cb.blobs))
	for idx, f := range cb.blobs {
		i := processChild(f, cs)
		// All children of compoundBlob must be Blobs, which get encoded and reffed by processChild.
		refs[idx] = i.(ref.Ref)
	}
	return enc.CompoundBlob{Offsets: cb.offsets, Blobs: refs}
}

func makeListEncodeable(l List, cs chunks.ChunkSink) interface{} {
	items := make([]interface{}, l.Len())
	for idx, f := range l.list {
		items[idx] = processChild(f, cs)
	}
	return items
}

func makeMapEncodeable(m Map, cs chunks.ChunkSink) interface{} {
	j := make([]interface{}, 0, 2*len(m.m))
	for _, r := range m.m {
		j = append(j, processChild(r.key, cs))
		j = append(j, processChild(r.value, cs))
	}
	return enc.MapFromItems(j...)
}

func makeSetEncodeable(s Set, cs chunks.ChunkSink) interface{} {
	items := make([]interface{}, s.Len())
	for idx, f := range s.m {
		items[idx] = processChild(f, cs)
	}
	return enc.SetFromItems(items...)
}

func processChild(f Future, cs chunks.ChunkSink) interface{} {
	if v, ok := f.(*unresolvedFuture); ok {
		return v.Ref()
	}

	v := f.Val()
	d.Exp.NotNil(v)
	switch v := v.(type) {
	// Blobs, lists, maps, and sets are always out-of-line
	case Blob, List, Map, Set:
		return WriteValue(v, cs)
	default:
		// Other types are always inline.
		return toEncodeable(v, cs)
	}
}

func (b Bool) ToPrimitive() interface{} {
	return bool(b)
}

func (i Int8) ToPrimitive() interface{} {
	return int8(i)
}

func (i Int16) ToPrimitive() interface{} {
	return int16(i)
}

func (i Int32) ToPrimitive() interface{} {
	return int32(i)
}

func (i Int64) ToPrimitive() interface{} {
	return int64(i)
}

func (f Float32) ToPrimitive() interface{} {
	return float32(f)
}

func (f Float64) ToPrimitive() interface{} {
	return float64(f)
}

func (u UInt8) ToPrimitive() interface{} {
	return uint8(u)
}

func (u UInt16) ToPrimitive() interface{} {
	return uint16(u)
}

func (u UInt32) ToPrimitive() interface{} {
	return uint32(u)
}

func (u UInt64) ToPrimitive() interface{} {
	return uint64(u)
}
