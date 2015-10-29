package newset

import (
	"github.com/attic-labs/noms/ref"
)

// SetBuilder creates a chunked Set implementation. It builds up a sequence of node, chunking them if necessary in the process.
type SetBuilder struct {
	current    node
	chunks     []node
	chunker    Chunker
	store      *nodeStore
	newChunker chunkerFactory
	newNode    nodeFactory
}

type nodeFactory func(st *nodeStore) node

func NewSetBuilder(store *nodeStore, newChunker chunkerFactory) SetBuilder {
	return SetBuilder{
		store:      store,
		chunker:    newChunker(),
		newChunker: newChunker,
		newNode:    func(st *nodeStore) node { return leaf{} },
	}
}

// Adds the next item to the builder. Items must be added in sort order.
func (builder *SetBuilder) AddItem(r ref.Ref) {
	builder.addEntry(r, r)
}

func (builder *SetBuilder) addEntry(first, r ref.Ref) {
	var newCurrent node
	if builder.current == nil {
		newCurrent = builder.newNode(builder.store).appendRef(first, r)
	} else {
		newCurrent = builder.current.appendRef(first, r)
	}
	builder.current = newCurrent
	builder.store.d[builder.current.ref()] = builder.current
	if builder.chunker.Add(r) {
		builder.chunks = append(builder.chunks, builder.current)
		builder.current = nil
	}
}

// Build returns the a Set with the canonical set structure of the added items.
func (builder *SetBuilder) Build() Set {
	if builder.current != nil {
		builder.chunks = append(builder.chunks, builder.current)
	}

	if len(builder.chunks) == 0 {
		// Nothing was added, this is an empty set.
		return Set{nil, builder.store, builder.newChunker}
	}

	if len(builder.chunks) == 1 {
		// No chunks were created, we're done.
		return Set{builder.chunks[0], builder.store, builder.newChunker}
	}

	// The set components chunked into multiple components. Now we chunk those.
	internalBuilder := &SetBuilder{
		store:      builder.store,
		chunker:    builder.newChunker(),
		newChunker: builder.newChunker,
		newNode:    func(st *nodeStore) node { return internal{store: st} },
	}
	for _, c := range builder.chunks {
		internalBuilder.addEntry(c.start(), c.ref())
	}
	return internalBuilder.Build()
}
