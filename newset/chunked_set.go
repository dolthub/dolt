package newset

import (
	"fmt"
	"sort"
	"strings"

	"github.com/attic-labs/noms/ref"
)

type chunkedSet struct {
	children entrySlice // sorted
}

type chunkedSetEntry struct {
	start ref.Ref
	set   Set
}

type entrySlice []chunkedSetEntry

func (es entrySlice) Len() int {
	return len(es)
}

func (es entrySlice) Less(i, j int) bool {
	return ref.Less(es[i].start, es[j].start)
}

func (es entrySlice) Swap(i, j int) {
	es[i], es[j] = es[j], es[i]
}

func (set chunkedSet) Len() (length uint64) {
	for _, entry := range set.children {
		length += entry.set.Len()
	}
	return
}

func (set chunkedSet) first() ref.Ref {
	return set.children[0].start
}

func (set chunkedSet) Has(r ref.Ref) bool {
	searchIndex := sort.Search(len(set.children), func(i int) bool {
		return ref.Greater(set.children[i].start, r)
	})
	if searchIndex == 0 {
		return false
	}
	searchIndex--
	return set.children[searchIndex].set.Has(r)
}

func (set chunkedSet) Ref() ref.Ref {
	// Eventually when chunked sets use noms Values this will need to be derived from the serialization of a chunked set, not simply a hash of all items' refs.
	h := ref.NewHash()
	for _, entry := range set.children {
		h.Write(entry.set.Ref().DigestSlice())
	}
	return ref.FromHash(h)
}

func (set chunkedSet) fmt(indent int) string {
	indentStr := strings.Repeat(" ", indent)
	if len(set.children) == 0 {
		return fmt.Sprintf("%s(empty chunked set)", indentStr)
	}
	s := fmt.Sprintf("%s(chunked with %d chunks)\n", indentStr, len(set.children))
	for i, entry := range set.children {
		s += fmt.Sprintf("%schunk %d (start %s)\n%s\n", indentStr, i, fmtRef(entry.start), entry.set.fmt(indent+4))
	}
	return s
}
