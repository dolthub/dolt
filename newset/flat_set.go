package newset

import (
	"fmt"
	"sort"
	"strings"

	"github.com/attic-labs/noms/ref"
)

type flatSet struct {
	d ref.RefSlice // sorted
	r *ref.Ref
}

func (s flatSet) Len() uint64 {
	return uint64(len(s.d))
}

func (s flatSet) Has(r ref.Ref) bool {
	idx := s.searchForIndex(r)
	return idx != len(s.d) && s.d[idx] == r
}

func (s flatSet) first() ref.Ref {
	return s.d[0]
}

func (s flatSet) Ref() ref.Ref {
	if s.r == nil {
		h := ref.NewHash()
		for _, r := range s.d {
			h.Write(r.DigestSlice())
		}
		r := ref.FromHash(h)
		s.r = &r
	}
	return *s.r
}

func (s flatSet) fmt(indent int) string {
	indentStr := strings.Repeat(" ", indent)
	if len(s.d) == 1 {
		return fmt.Sprintf("%sflat %s", indentStr, fmtRef(s.d[0]))
	}
	return fmt.Sprintf("%sflat{%s...(%d more)...%s}", indentStr, fmtRef(s.d[0]), len(s.d)-2, fmtRef(s.d[len(s.d)-1]))
}

func (s flatSet) searchForIndex(r ref.Ref) int {
	return sort.Search(len(s.d), func(i int) bool {
		return !ref.Less(s.d[i], r)
	})
}

func fmtRef(r ref.Ref) string {
	str := r.String()
	return str[len(str)-8:]
}
