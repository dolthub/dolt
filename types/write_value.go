package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// ValueWriter is an interface that knows how to write Noms Values, e.g. datas/DataStore. Required to avoid import cycle between this package and the package that implements Value writing.
type ValueWriter interface {
	WriteValue(v Value) ref.Ref
}

type primitive interface {
	ToPrimitive() interface{}
}

// WriteValue takes a Value, encodes it into Chunks, and puts them into cs. As a part of BUG 654, we're trying to get rid of the need to provide a ChunkSink here.
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
	encode(w, e)
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
	case Package:
		processPackageChildren(v, cs)
	}
	return encNomsValue(v, cs)
}

func processPackageChildren(p Package, cs chunks.ChunkSink) {
	for _, r := range p.dependencies {
		p := LookupPackage(r)
		if p != nil {
			writeChildValueInternal(*p, cs)
		}
	}
}
