package newset

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// This file is a giant copy-paste, but the architecture of chunking will likely be written in terms of iteration, so deal with it then.
type SetBuilder interface {
	AddItem(r ref.Ref)
	Build() Set
}

type leafSetBuilder struct {
	current flatSet
	chunks  []flatSet
	chunker Chunker
}

func NewSetBuilder() SetBuilder {
	return NewSetBuilderWithChunker(newBuzChunker())
}

func NewSetBuilderWithChunker(chunker Chunker) SetBuilder {
	return &leafSetBuilder{chunker: chunker}
}

func (builder *leafSetBuilder) AddItem(r ref.Ref) {
	builder.current.d = append(builder.current.d, r)
	if builder.chunker.Add(r) {
		builder.chunks = append(builder.chunks, builder.current)
		builder.current = flatSet{}
	}
}

func (builder *leafSetBuilder) Build() Set {
	if builder.current.Len() > uint64(0) {
		builder.chunks = append(builder.chunks, builder.current)
	}

	if len(builder.chunks) == 1 {
		d.Chk.NotEqual(0, builder.chunks[0].Len())
		return builder.chunks[0]
	}

	mcb := newMetaChunkBuilder(builder.chunker.New())
	for _, c := range builder.chunks {
		mcb.AddItem(c)
	}

	return mcb.Build()
}

type chunkedSetBuilder struct {
	current chunkedSet
	sets    []chunkedSet
	chunker Chunker
}

func newMetaChunkBuilder(chunker Chunker) chunkedSetBuilder {
	return chunkedSetBuilder{chunker: chunker}
}

func (mcb *chunkedSetBuilder) AddItem(s Set) {
	mcb.current.children = append(mcb.current.children, chunkedSetEntry{s.first(), s})
	if mcb.chunker.Add(s.Ref()) {
		mcb.sets = append(mcb.sets, mcb.current)
		mcb.current = chunkedSet{}
	}
}

func (mcb *chunkedSetBuilder) Build() chunkedSet {
	if mcb.current.Len() > 0 {
		mcb.sets = append(mcb.sets, mcb.current)
	}

	if len(mcb.sets) == 1 {
		d.Chk.NotEqual(0, mcb.sets[0].Len())
		return mcb.sets[0]
	}

	b := newMetaChunkBuilder(mcb.chunker.New())
	for _, s := range mcb.sets {
		b.AddItem(s)
	}
	return b.Build()
}
