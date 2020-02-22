package setalgebra

import (
	"errors"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func finiteSetUnion(fs1, fs2 FiniteSet) (FiniteSet, error) {
	hashToVal := make(map[hash.Hash]types.Value, len(fs1.HashToVal)+len(fs2.HashToVal))
	for h, v := range fs1.HashToVal {
		hashToVal[h] = v
	}

	for h, v := range fs2.HashToVal {
		hashToVal[h] = v
	}

	return FiniteSet{HashToVal: hashToVal}, nil
}

func copyIntevalEndpoint(ep *IntervalEndpoint) *IntervalEndpoint {
	if ep == nil {
		return nil
	}

	copyOf := *ep

	return &copyOf
}

func finiteSetIntervalUnion(fs FiniteSet, in Interval) (Set, error) {
	inStart := copyIntevalEndpoint(in.Start)
	inEnd := copyIntevalEndpoint(in.End)

	hashToVal := make(map[hash.Hash]types.Value, len(fs.HashToVal))
	for h, v := range fs.HashToVal {
		inRange, err := ValueInInterval(in, v)

		if err != nil {
			return nil, err
		}

		if !inRange {
			if inStart != nil && !inStart.Inclusive {
				if inStart.Val.Equals(v) {
					inStart.Inclusive = true
					continue
				}
			}

			if in.End != nil && !in.End.Inclusive {
				if in.End.Val.Equals(v) {
					inEnd.Inclusive = true
					continue
				}
			}

			hashToVal[h] = v
		}
	}

	resInterval := Interval{in.nbf, inStart, inEnd}
	if len(hashToVal) > 0 {
		newSet := FiniteSet{HashToVal: hashToVal}
		return CompositeSet{newSet, []Interval{resInterval}}, nil
	} else {
		return resInterval, nil
	}
}

func finiteSetCompositeSetUnion(fs FiniteSet, cs CompositeSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value, len(fs.HashToVal))
	for h, v := range cs.Set.HashToVal {
		hashToVal[h] = v
	}

	for h, v := range fs.HashToVal {
		var inRange bool
		var err error
		for _, r := range cs.Intervals {
			inRange, err = ValueInInterval(r, v)

			if err != nil {
				return nil, err
			}

			if inRange {
				break
			}
		}

		if !inRange {
			hashToVal[h] = v
		}
	}

	return CompositeSet{FiniteSet{hashToVal}, cs.Intervals}, nil
}

func intervalUnion(in1, in2 Interval) (Set, error) {
	intComparison, err := compareIntervals(in1, in2)

	if err != nil {
		return nil, err
	}

	return intervalUnionWithComparison(in1, in2, intComparison)
}

func intervalUnionWithComparison(in1, in2 Interval, intComparison intervalComparison) (Set, error) {
	var resIntervToReduce Interval
	if intComparison == noOverlapLess {
		if in1.End != nil && in2.Start != nil && (in1.End.Inclusive || in2.Start.Inclusive) && in1.End.Val.Equals(in2.Start.Val) {
			resIntervToReduce = Interval{in1.nbf, in1.Start, in2.End}
		} else {
			return CompositeSet{FiniteSet{make(map[hash.Hash]types.Value)}, []Interval{in1, in2}}, nil
		}
	} else if intComparison == noOverlapGreater {
		if in2.End != nil && in1.Start != nil && (in2.End.Inclusive || in1.Start.Inclusive) && in2.End.Val.Equals(in1.Start.Val) {
			resIntervToReduce = Interval{in1.nbf, in2.Start, in1.End}
		} else {
			return CompositeSet{FiniteSet{make(map[hash.Hash]types.Value)}, []Interval{in2, in1}}, nil
		}
	} else if intComparison[start1start2] <= 0 {
		if intComparison[end1end2] >= 0 {
			return in1, nil
		} else {
			resIntervToReduce = Interval{in1.nbf, in1.Start, in2.End}
		}
	} else {
		if intComparison[end1end2] <= 0 {
			return in2, nil
		} else {
			resIntervToReduce = Interval{in1.nbf, in2.Start, in1.End}
		}
	}

	return simplifyInterval(resIntervToReduce)
}

func intervalCompositeSetUnion(in Interval, cs CompositeSet) (Set, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	for h, v := range cs.Set.HashToVal {
		contained, err := ValueInInterval(in, v)

		if err != nil {
			return nil, err
		}

		if !contained {
			hashToVal[h] = v
		}
	}

	intervals, err := unionWithMultipleIntervals(in, cs.Intervals)

	if err == errUniversalSetErr {
		return UniversalSet{}, nil
	} else if err != nil {
		return nil, err
	}

	if len(hashToVal) == 0 && len(intervals) == 1 {
		// could possibly be universal set
		return simplifyInterval(intervals[0])
	} else {
		return CompositeSet{FiniteSet{hashToVal}, intervals}, nil
	}
}

var errUniversalSetErr = errors.New("resulting interval is the universal set")

func unionWithMultipleIntervals(in Interval, src []Interval) ([]Interval, error) {
	dest := make([]Interval, 0, len(src)+1)
	for i, curr := range src {
		intComparison, err := compareIntervals(in, curr)

		if err != nil {
			return nil, err
		}

		if intComparison == noOverlapLess {
			if in.End != nil && curr.Start != nil && (in.End.Inclusive || curr.Start.Inclusive) && in.End.Val.Equals(curr.Start.Val) {
				in = Interval{in.nbf, in.Start, curr.End}
				continue
			}

			// current interval is before all remaining intervals
			dest = append(dest, in)
			in = Interval{}
			dest = append(dest, src[i:]...)
			break
		} else if intComparison == noOverlapGreater {
			if curr.End != nil && in.Start != nil && (curr.End.Inclusive || in.Start.Inclusive) && curr.End.Val.Equals(in.Start.Val) {
				in = Interval{in.nbf, curr.Start, in.End}
				continue
			}

			dest = append(dest, curr)
		} else {
			un, err := intervalUnionWithComparison(in, curr, intComparison)

			if err != nil {
				return nil, err
			}

			switch typedVal := un.(type) {
			case UniversalSet:
				return nil, errUniversalSetErr
			case Interval:
				in = typedVal
			default:
				panic("Should not be possible.")
			}
		}
	}

	if in.nbf != nil {
		dest = append(dest, in)
	}

	return dest, nil
}

func addIfNotInIntervals(hashToValue map[hash.Hash]types.Value, fs FiniteSet, intervals []Interval) error {
	var err error
	for h, v := range fs.HashToVal {
		var found bool
		for _, in := range intervals {
			found, err = ValueInInterval(in, v)
			if err != nil {
				return err
			}

			if found {
				break
			}
		}

		if !found {
			hashToValue[h] = v
		}
	}

	return nil
}

func findUniqueFiniteSetForComposites(cs1, cs2 CompositeSet) (FiniteSet, error) {
	hashToVal := make(map[hash.Hash]types.Value)
	err := addIfNotInIntervals(hashToVal, cs1.Set, cs2.Intervals)

	if err != nil {
		return FiniteSet{}, err
	}

	err = addIfNotInIntervals(hashToVal, cs2.Set, cs1.Intervals)

	if err != nil {
		return FiniteSet{}, err
	}

	return FiniteSet{hashToVal}, nil
}

func compositeUnion(cs1, cs2 CompositeSet) (Set, error) {
	fs, err := findUniqueFiniteSetForComposites(cs1, cs2)

	if err != nil {
		return nil, err
	}

	intervals := cs1.Intervals
	for _, currInterval := range cs2.Intervals {
		intervals, err = unionWithMultipleIntervals(currInterval, intervals)

		if err == errUniversalSetErr {
			return UniversalSet{}, nil
		} else if err != nil {
			return nil, err
		}
	}

	if len(fs.HashToVal) == 0 && len(intervals) == 1 {
		// could possibly be universal set
		return simplifyInterval(intervals[0])
	} else {
		return CompositeSet{fs, intervals}, nil
	}
}
