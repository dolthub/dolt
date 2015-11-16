package newset

import (
	"fmt"
	"sort"
	"strings"

	"github.com/attic-labs/noms/ref"
)

// A slice of internalEntry make up a single internal. Each entry contains enough information to find a set value in log(n) complexity: the smallest value in its subtree, and a ref to its subtree.
type internalEntry struct {
	start ref.Ref // the smallest set value in this entry's tree
	r     ref.Ref // the ref to the root of this entry's tree
}

type entrySlice []internalEntry

func (es entrySlice) Len() int {
	return len(es)
}

func (es entrySlice) Less(i, j int) bool {
	return es[i].start.Less(es[j].start)
}

func (es entrySlice) Swap(i, j int) {
	es[i], es[j] = es[j], es[i]
}

// internal is a node which acts as an index to other nodes.
type internal struct {
	store    *nodeStore
	children entrySlice
	r        ref.Ref
}

func newInternal(store *nodeStore, children entrySlice) internal {
	// Eventually when chunked sets use noms Values this will need to be derived from the serialization of a chunked set, not simply a hash of all items' refs.
	h := ref.NewHash()
	for _, internalEntry := range children {
		h.Write(internalEntry.r.DigestSlice())
	}
	return internal{store, children, ref.FromHash(h)}
}

func (node internal) length() (l uint64) {
	for _, internalEntry := range node.children {
		entryComponent := node.store.d[internalEntry.r]
		l += entryComponent.length()
	}
	return
}

func (node internal) start() ref.Ref {
	return node.children[0].start
}

func (node internal) has(r ref.Ref) bool {
	searchIndex := sort.Search(len(node.children), func(i int) bool {
		return node.children[i].start.Greater(r)
	})
	if searchIndex == 0 {
		return false
	}
	searchIndex--
	internalEntry := node.children[searchIndex]
	entryComponent, ok := node.store.d[internalEntry.r]
	return ok && entryComponent.has(r)
}

func (node internal) appendRef(start, r ref.Ref) node {
	return newInternal(node.store, append(node.children, internalEntry{start, r}))
}

func (node internal) ref() ref.Ref {
	return node.r
}

func (node internal) iter(cb func(int, ref.Ref)) {
	for _, internalEntry := range node.children {
		node.store.d[internalEntry.r].iter(cb)
	}
}

func (node internal) fmt(indent int) string {
	indentStr := strings.Repeat(" ", indent)
	if len(node.children) == 0 {
		return fmt.Sprintf("%s(empty chunked set)", indentStr)
	}
	s := fmt.Sprintf("%s(chunked with %d chunks)\n", indentStr, len(node.children))
	for i, internalEntry := range node.children {
		entryComponent := node.store.d[internalEntry.r]
		s += fmt.Sprintf("%schunk %d (start %s)\n%s\n", indentStr, i, fmtRef(internalEntry.start), entryComponent.fmt(indent+4))
	}
	return s
}
