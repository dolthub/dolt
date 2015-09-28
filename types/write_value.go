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

type NomsValue interface {
	NomsValue() Value
	TypeRef() TypeRef
}

func WriteValue(v interface{}, cs chunks.ChunkSink) ref.Ref {
	d.Chk.NotNil(cs)
	return writeValueInternal(v, cs)
}

func writeChildValueInternal(v Value, cs chunks.ChunkSink) ref.Ref {
	if cs == nil {
		return v.Ref()
	}

	return writeValueInternal(v, cs)
}

func writeValueInternal(v interface{}, cs chunks.ChunkSink) ref.Ref {
	e := toEncodeable(v, cs)
	w := chunks.NewChunkWriter()
	enc.Encode(w, e)
	c := w.Chunk()
	if cs != nil {
		cs.Put(c)
	}
	return c.Ref()
}

func toEncodeable(v interface{}, cs chunks.ChunkSink) interface{} {
	switch v := v.(type) {
	case blobLeaf:
		return v.Reader()
	case compoundBlob:
		return encCompoundBlobFromCompoundBlob(v, cs)
	case NomsValue:
		return encNomsValue(v, cs)
	case compoundList:
		return encCompoundListFromCompoundList(v, cs)
	case listLeaf:
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
	case TypeRef:
		return makeTypeEncodeable(v, cs)
	default:
		return v
	}
}

func encCompoundBlobFromCompoundBlob(cb compoundBlob, cs chunks.ChunkSink) interface{} {
	refs := make([]ref.Ref, len(cb.futures))
	for idx, f := range cb.futures {
		i := processChild(f, cs)
		// All children of compoundBlob must be Blobs, which get encoded and reffed by processChild.
		refs[idx] = i.(ref.Ref)
	}
	return enc.CompoundBlob{Offsets: cb.offsets, Blobs: refs}
}

func encCompoundListFromCompoundList(cl compoundList, cs chunks.ChunkSink) interface{} {
	refs := make([]ref.Ref, len(cl.futures))
	for idx, f := range cl.futures {
		i := processChild(f, cs)
		// All children of compoundList must be Lists, which get encoded and reffed by processChild.
		refs[idx] = i.(ref.Ref)
	}
	return enc.CompoundList{Offsets: cl.offsets, Lists: refs}
}

func makeListEncodeable(l listLeaf, cs chunks.ChunkSink) interface{} {
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

func makeTypeEncodeable(t TypeRef, cs chunks.ChunkSink) interface{} {
	pkgRef := t.PackageRef()
	p := LookupPackage(pkgRef)
	if p != nil {
		pkgRef = writeChildValueInternal(p.NomsValue(), cs)
	}
	return enc.TypeRef{PkgRef: pkgRef, Name: t.Name(), Kind: uint8(t.Kind()), Desc: toEncodeable(t.Desc.ToValue(), cs)}
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
		return writeChildValueInternal(v, cs)
	default:
		// Other types are always inline.
		return toEncodeable(v, cs)
	}
}
