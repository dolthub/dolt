package index

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

func ClosedRange(tpl1, tpl2 types.Tuple) *noms.ReadRange {
	return CustomRange(tpl1, tpl2, sql.Closed, sql.Closed)
}

func OpenRange(tpl1, tpl2 types.Tuple) *noms.ReadRange {
	return CustomRange(tpl1, tpl2, sql.Open, sql.Open)
}

func CustomRange(tpl1, tpl2 types.Tuple, bt1, bt2 sql.RangeBoundType) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl1.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		if bt1 == sql.Closed {
			nrc = append(nrc, columnBounds{
				boundsCase: boundsCase_greaterEquals_infinity,
				lowerbound: tupleVal,
			})
		} else {
			nrc = append(nrc, columnBounds{
				boundsCase: boundsCase_greater_infinity,
				lowerbound: tupleVal,
			})
		}
		return false, nil
	})
	_ = tpl2.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		idx := (tupleIndex - 1) / 2
		if bt2 == sql.Closed {
			// Bounds cases are enum aliases on bytes, and they're arranged such that we can increment the case
			// that was previously set when evaluating the lowerbound to get the proper overall case.
			nrc[idx].boundsCase += 1
			nrc[idx].upperbound = tupleVal
		} else {
			nrc[idx].boundsCase += 2
			nrc[idx].upperbound = tupleVal
		}
		return false, nil
	})
	return &noms.ReadRange{
		Start:     tpl1,
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func GreaterThanRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_greater_infinity,
			lowerbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     tpl,
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func LessThanRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_infinity_less,
			upperbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     types.EmptyTuple(types.Format_Default),
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func GreaterOrEqualRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_greaterEquals_infinity,
			lowerbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     tpl,
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func LessOrEqualRange(tpl types.Tuple) *noms.ReadRange {
	var nrc nomsRangeCheck
	_ = tpl.IterFields(func(tupleIndex uint64, tupleVal types.Value) (stop bool, err error) {
		if tupleIndex%2 == 0 {
			return false, nil
		}
		nrc = append(nrc, columnBounds{
			boundsCase: boundsCase_infinity_lessEquals,
			upperbound: tupleVal,
		})
		return false, nil
	})
	return &noms.ReadRange{
		Start:     types.EmptyTuple(types.Format_Default),
		Inclusive: true,
		Reverse:   false,
		Check:     nrc,
	}
}

func AllRange() *noms.ReadRange {
	return &noms.ReadRange{
		Start:     types.EmptyTuple(types.Format_Default),
		Inclusive: true,
		Reverse:   false,
		Check:     nomsRangeCheck{},
	}
}

func ReadRangesEqual(nr1, nr2 *noms.ReadRange) bool {
	if nr1 == nil || nr2 == nil {
		if nr1 == nil && nr2 == nil {
			return true
		}
		return false
	}
	if nr1.Inclusive != nr2.Inclusive || nr1.Reverse != nr2.Reverse || !nr1.Start.Equals(nr2.Start) ||
		!nr1.Check.(nomsRangeCheck).Equals(nr2.Check.(nomsRangeCheck)) {
		return false
	}
	return true
}

func ReadRangesFromIndexLookup(lookup sql.IndexLookup) []*noms.ReadRange {
	return lookup.(*doltIndexLookup).ranges
}
