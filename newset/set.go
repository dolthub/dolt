package newset

import "github.com/attic-labs/noms/ref"

// Set is the noms set type. It's implemented by wrapping a node.
type Set struct {
	root       node
	store      *nodeStore
	newChunker chunkerFactory
}

func NewSet(store *nodeStore, newChunker chunkerFactory) Set {
	return Set{nil, store, newChunker}
}

// Len returns the length of this set.
func (s Set) Len() uint64 {
	if s.root == nil {
		return uint64(0)
	}
	return s.root.length()
}

// Has returns true if this set contains r, false if it doesn't.
func (s Set) Has(r ref.Ref) bool {
	if s.root == nil {
		return false
	}
	return s.root.has(r)
}

// Put returns a new Set with r added to it. The new Set may be equivalent to this if this already contains r.
func (s Set) Put(put ref.Ref) Set {
	sb := NewSetBuilder(s.store, s.newChunker)
	has := false
	// Rebuild entire set, a very wasteful way to implement Put. Issue#475 tracks the development of newset including how to implement Put efficiently.
	if s.root != nil {
		s.root.iter(func(i int, r ref.Ref) {
			switch {
			case has:
			case put == r:
				has = true
			case ref.Less(put, r):
				sb.AddItem(put)
				has = true
			}
			sb.AddItem(r)
		})
	}
	if !has {
		sb.AddItem(put)
	}
	return sb.Build()
}

// Ref returns the ref of this set, or ref.Ref{} if this set is empty.
func (s Set) Ref() ref.Ref {
	if s.root == nil {
		return ref.Ref{}
	}
	return s.root.ref()
}

// Fmt returns a nicely formatted string representation of this set for debugging.
func (s Set) Fmt() string {
	if s.root == nil {
		return "(empty set)"
	}
	return s.root.fmt(0)
}
