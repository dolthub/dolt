package types

func (p Package) GetOrdinal(n string) (ordinal int64) {
	ordinal = -1
	p.Types().Iter(func(tr TypeRef, i uint64) (stop bool) {
		if tr.Name() == n && tr.Namespace() == "" {
			ordinal = int64(i)
			stop = true
		}
		return
	})
	return
}
