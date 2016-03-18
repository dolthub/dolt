package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

// ValueWriter is an interface that knows how to write Noms Values, e.g. datas/DataStore. Required to avoid import cycle between this package and the package that implements Value writing.
type ValueWriter interface {
	WriteValue(v Value) ref.Ref
}

type primitive interface {
	ToPrimitive() interface{}
}

// WriteValue takes a Value and encodes it to a Chunk, if |vw| is non-nill, it will vw.WriteValue reachable unwritten sub-chunks.
func EncodeValue(v Value, vw ValueWriter) chunks.Chunk {
	e := toEncodeable(v, vw)
	w := chunks.NewChunkWriter()
	encode(w, e)
	return w.Chunk()
}

func toEncodeable(v Value, vw ValueWriter) interface{} {
	switch v := v.(type) {
	case blobLeaf:
		return v.Reader()
	case Package:
		processPackageChildren(v, vw)
	}
	return encNomsValue(v, vw)
}

func processPackageChildren(p Package, vw ValueWriter) {
	if vw == nil {
		return
	}

	for _, r := range p.dependencies {
		p := LookupPackage(r)
		if p != nil && vw != nil {
			vw.WriteValue(*p)
		}
	}
}
