package setalgebra

type CompositeSet struct {
	Set       FiniteSet
	Intervals []Interval
}

func (cs CompositeSet) Union(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetCompositeSetUnion(otherTyped, cs)
	case Interval:
		return intervalCompositeSetUnion(otherTyped, cs)
	case CompositeSet:
		return compositeUnion(cs, otherTyped)
	case EmptySet:
		return cs, nil
	case UniversalSet:
		return otherTyped, nil
	}

	panic("unknown set type")

}

func (cs CompositeSet) Intersect(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetCompositeSetIntersection(otherTyped, cs)
	case Interval:
		return intervalCompositeSetIntersection(otherTyped, cs)
	case CompositeSet:
		return compositeIntersection(otherTyped, cs)
	case EmptySet:
		return EmptySet{}, nil
	case UniversalSet:
		return cs, nil
	}

	panic("unknown set type")
}
