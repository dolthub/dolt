package types

func (p Package) maybeGetNamedType(n string) (t TypeRef, found bool) {
	p.Types().Iter(func(tr TypeRef, i uint64) (stop bool) {
		if tr.Name() == n {
			t = tr
			found = true
			stop = true
		}
		return
	})
	return
}

func (p Package) GetNamedType(n string) TypeRef {
	t, _ := p.maybeGetNamedType(n)
	return t
}

func (p Package) HasNamedType(n string) bool {
	_, b := p.maybeGetNamedType(n)
	return b
}
