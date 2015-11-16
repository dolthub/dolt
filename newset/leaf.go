package newset

import (
	"fmt"
	"sort"
	"strings"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

// leaf is a node which contains the values of the set.
type leaf struct {
	d ref.RefSlice // sorted
	r ref.Ref
}

func newLeaf(d ref.RefSlice) leaf {
	// Eventually when chunked sets use noms Values this will need to be derived from the serialization of a chunked set, not simply a hash of all items' refs.
	h := ref.NewHash()
	for _, r := range d {
		h.Write(r.DigestSlice())
	}
	return leaf{d, ref.FromHash(h)}
}

func (node leaf) length() uint64 {
	return uint64(len(node.d))
}

func (node leaf) start() ref.Ref {
	return node.d[0]
}

func (node leaf) has(r ref.Ref) bool {
	idx := node.searchForIndex(r)
	return idx != len(node.d) && node.d[idx] == r
}

func (node leaf) appendRef(first, r ref.Ref) node {
	// r is a value, not a reference to a node, so first entry == only entry == r.
	d.Chk.Equal(first, r)
	return newLeaf(append(node.d, r))
}

func (node leaf) ref() ref.Ref {
	return node.r
}

func (node leaf) iter(cb func(int, ref.Ref)) {
	for i, r := range node.d {
		cb(i, r)
	}
}

func (node leaf) fmt(indent int) string {
	indentStr := strings.Repeat(" ", indent)
	if len(node.d) == 1 {
		return fmt.Sprintf("%sflat %s", indentStr, fmtRef(node.d[0]))
	}
	return fmt.Sprintf("%sflat{%s...(%d more)...%s}", indentStr, fmtRef(node.d[0]), len(node.d)-2, fmtRef(node.d[len(node.d)-1]))
}

func (node leaf) searchForIndex(r ref.Ref) int {
	return sort.Search(len(node.d), func(i int) bool {
		return !node.d[i].Less(r)
	})
}

func fmtRef(r ref.Ref) string {
	str := r.String()
	return str[len(str)-8:]
}
