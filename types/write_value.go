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
	return writeValueInternal(v, cs)
}

func writeChildValueInternal(v Value, cs chunks.ChunkSink) ref.Ref {
	if cs == nil {
		return v.Ref()
	}

	return writeValueInternal(v, cs)
}

func writeValueInternal(v Value, cs chunks.ChunkSink) ref.Ref {
	e := toEncodeable(v, cs)
	w := chunks.NewChunkWriter()
	enc.Encode(w, e)
	c := w.Chunk()
	if cs != nil {
		cs.Put(c)
	}
	return c.Ref()
}

func toEncodeable(v Value, cs chunks.ChunkSink) interface{} {
	switch v := v.(type) {
	case blobLeaf:
		return v.Reader()
	case compoundBlob:
		tv := processCompoundBlob(v, cs)
		return encNomsValue(tv, cs)
	case Package:
		processPackageChildren(v, cs)
	}
	return encNomsValue(v, cs)
}

func processCompoundBlob(cb compoundBlob, cs chunks.ChunkSink) compoundBlobStruct {
	refs := make([]ref.Ref, len(cb.futures))
	for idx, f := range cb.futures {
		processChild(f, cs)
		refs[idx] = f.Ref()
	}
	return compoundBlobStructDef{
		Offsets: cb.offsets,
		Blobs:   refs,
	}.New()
}

func processPackageChildren(p Package, cs chunks.ChunkSink) {
	for _, r := range p.dependencies {
		p := LookupPackage(r)
		if p != nil {
			writeChildValueInternal(*p, cs)
		}
	}
}

func processChild(f Future, cs chunks.ChunkSink) interface{} {
	if v, ok := f.(*unresolvedFuture); ok {
		return v.Ref()
	}

	v := f.Val()
	d.Exp.NotNil(v)
	return writeChildValueInternal(v, cs)
}
