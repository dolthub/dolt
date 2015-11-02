package types

import (
	"github.com/attic-labs/noms/ref"
)

type targetRef interface {
	TargetRef() ref.Ref
}

func appendChunk(chunks []ref.Ref, v Value) []ref.Ref {
	if v.TypeRef().Kind() == RefKind {
		chunks = append(chunks, v.(targetRef).TargetRef())
	}
	return chunks
}
