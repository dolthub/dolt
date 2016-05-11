package types

import "github.com/attic-labs/noms/chunks"

// ValueWriter is an interface that knows how to write Noms Values, e.g. datas/Database. Required to avoid import cycle between this package and the package that implements Value writing.
type ValueWriter interface {
	WriteValue(v Value) Ref
}

type primitive interface {
	ToPrimitive() interface{}
}

// EncodeValue takes a Value and encodes it to a Chunk, if |vw| is non-nill, it will vw.WriteValue reachable unwritten sub-chunks.
func EncodeValue(v Value, vw ValueWriter) chunks.Chunk {
	e := toEncodeable(v, vw)
	w := chunks.NewChunkWriter()
	encode(w, e)
	return w.Chunk()
}

func toEncodeable(v Value, vw ValueWriter) interface{} {
	if b, ok := v.(Blob); ok {
		if _, ok := b.sequence().(blobLeafSequence); ok {
			return b.Reader()
		}
	}

	return encNomsValue(v, vw)
}
