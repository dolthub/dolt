package setalgebra

type Set interface {
	Union(other Set) (Set, error)
	Intersect(other Set) (Set, error)
}

type EmptySet struct{}

func (es EmptySet) Union(other Set) (Set, error) {
	return other, nil
}

func (es EmptySet) Intersect(other Set) (Set, error) {
	return es, nil
}

type UniversalSet struct{}

func (us UniversalSet) Union(other Set) (Set, error) {
	return us, nil
}

func (us UniversalSet) Intersect(other Set) (Set, error) {
	return other, nil
}
