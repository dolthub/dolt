// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package setalgebra

import "github.com/liquidata-inc/dolt/go/store/types"

type IntervalEndpoint struct {
	Val       types.Value
	Inclusive bool
}

type Interval struct {
	nbf   *types.NomsBinFormat
	Start *IntervalEndpoint
	End   *IntervalEndpoint
}

func NewInterval(nbf *types.NomsBinFormat, start, end *IntervalEndpoint) Interval {
	return Interval{nbf, start, end}
}

func (in Interval) Union(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetIntervalUnion(otherTyped, in)
	case Interval:
		return intervalUnion(in, otherTyped)
	case CompositeSet:
		return intervalCompositeSetUnion(in, otherTyped)
	case EmptySet:
		return in, nil
	case UniversalSet:
		return otherTyped, nil
	}

	panic("unknown set type")

}

func (in Interval) Intersect(other Set) (Set, error) {
	switch otherTyped := other.(type) {
	case FiniteSet:
		return finiteSetIntervalIntersection(otherTyped, in)
	case Interval:
		return intervalIntersection(in, otherTyped)
	case CompositeSet:
		return intervalCompositeSetIntersection(in, otherTyped)
	case EmptySet:
		return EmptySet{}, nil

	case UniversalSet:
		return in, nil
	}

	panic("unknown set type")

}

func ValueInInterval(in Interval, val types.Value) (bool, error) {
	if in.Start == nil && in.End == nil {
		// interval is open on both sides. full range
		return true, nil
	}

	var nbf *types.NomsBinFormat
	matchesStartCondition := func() (bool, error) {
		res, err := in.Start.Val.Less(nbf, val)

		if err != nil {
			return false, err
		}

		if res {
			return res, nil
		}

		if in.Start.Inclusive {
			return in.Start.Val.Equals(val), nil
		}

		return false, nil
	}

	matchesEndCondition := func() (bool, error) {
		res, err := val.Less(nbf, in.End.Val)

		if err != nil {
			return false, err
		}

		if res {
			return res, nil
		}

		if in.End.Inclusive {
			return in.End.Val.Equals(val), nil
		}

		return false, nil
	}

	if in.Start != nil && in.End != nil {
		st, err := matchesStartCondition()

		if err != nil {
			return false, err
		}

		end, err := matchesEndCondition()

		if err != nil {
			return false, err
		}

		return st && end, nil
	} else if in.Start != nil {
		return matchesStartCondition()
	} else {
		return matchesEndCondition()
	}
}

const (
	start1start2 int = iota
	start1end2
	end1start2
	end1end2
)

var noOverlapLess = intervalComparison{-1, -1, -1, -1}
var noOverlapGreater = intervalComparison{1, 1, 1, 1}

type intervalComparison [4]int

func compareIntervals(in1, in2 Interval) (intervalComparison, error) {
	comp := intervalComparison{}
	endpoints := [2][2]*IntervalEndpoint{{in1.Start, in1.End}, {in2.Start, in2.End}}

	var err error
	for i := 0; i < 2; i++ {
		for j := 0; j < 2; j++ {
			ep1, ep2 := endpoints[0][i], endpoints[1][j]
			lt, eq := false, false

			if ep1 == nil && ep2 == nil {
				if i == j {
					eq = true
				} else {
					lt = i < j
				}
			} else if ep1 == nil {
				lt = i == 0
			} else if ep2 == nil {
				lt = j == 1
			} else {
				eq = ep1.Val.Equals(ep2.Val)

				if eq {
					if !ep1.Inclusive && !ep2.Inclusive {
						if i != j {
							eq = false
							lt = i > j
						}
					} else if !ep1.Inclusive {
						eq = false
						lt = i == 1
					} else if !ep2.Inclusive {
						eq = false
						lt = j == 0
					}
				} else {
					lt, err = ep1.Val.Less(in1.nbf, ep2.Val)

					if err != nil {
						return intervalComparison{}, err
					}
				}
			}

			var res int
			if lt {
				res = -1
			} else if !eq {
				res = 1
			}

			resIdx := i*2 + j
			comp[resIdx] = res
		}
	}

	return comp, nil
}

func simplifyInterval(in Interval) (Set, error) {
	if in.Start == nil && in.End == nil {
		return UniversalSet{}, nil
	} else if in.Start != nil && in.End != nil {
		if in.Start.Val.Equals(in.End.Val) {
			return NewFiniteSet(in.nbf, in.Start.Val)
		}
	}

	return in, nil
}
