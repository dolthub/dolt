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

package expreval

import (
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	intCat int = iota
	uintCat
	floatCat
	stringCat
	dateCat
)

func compareLiterals(l1, l2 *expression.Literal) (int, error) {
	cat1 := categorizeType(l1)
	cat2 := categorizeType(l2)

	compareCat := intCat
	if cat1 == cat2 {
		compareCat = cat1
	} else if cat1 == stringCat || cat2 == stringCat || cat1 == dateCat || cat2 == dateCat {
		compareCat = stringCat
	} else if cat1 == floatCat || cat2 == floatCat {
		compareCat = floatCat
	} else if cat1 == uintCat || cat2 == uintCat {
		compareCat = uintCat
	}

	switch compareCat {
	case intCat:
		v1, err := literalAsInt64(l1)

		if err != nil {
			return 0, err
		}

		v2, err := literalAsInt64(l2)

		if err != nil {
			return 0, err
		}

		switch {
		case v1 > v2:
			return 1, nil
		case v1 < v2:
			return -1, nil
		default:
			return 0, nil
		}

	case uintCat:
		v1, err := literalAsUint64(l1)

		if err != nil {
			return 0, err
		}

		v2, err := literalAsUint64(l2)

		if err != nil {
			return 0, err
		}

		switch {
		case v1 > v2:
			return 1, nil
		case v1 < v2:
			return -1, nil
		default:
			return 0, nil
		}

	case floatCat:
		v1, err := literalAsFloat64(l1)

		if err != nil {
			return 0, err
		}

		v2, err := literalAsFloat64(l2)

		if err != nil {
			return 0, err
		}

		switch {
		case v1 > v2:
			return 1, nil
		case v1 < v2:
			return -1, nil
		default:
			return 0, nil
		}

	case stringCat:
		v1, err := literalAsString(l1)

		if err != nil {
			return 0, err
		}

		v2, err := literalAsString(l2)

		if err != nil {
			return 0, err
		}

		return strings.Compare(v1, v2), nil

	case dateCat:
		v1, err := literalAsTimestamp(l1)

		if err != nil {
			return 0, err
		}

		v2, err := literalAsTimestamp(l2)

		if err != nil {
			return 0, err
		}

		diff := v1.Sub(v2).Seconds()

		switch {
		case diff > 0:
			return 1, nil
		case diff < 0:
			return -1, nil
		default:
			return 0, nil
		}
	}

	return 0, errUnsupportedComparisonType.New()
}

func categorizeType(l *expression.Literal) int {
	switch l.Type() {
	case sql.Int8, sql.Int16, sql.Int32, sql.Int64, sql.Boolean, sql.Uint8, sql.Uint16, sql.Uint32:
		return intCat
	case sql.Uint64:
		u64 := l.Value().(uint64)
		if u64&0xF000000000000000 != 0 {
			return uintCat
		}
		return intCat
	case sql.Float32, sql.Float64:
		return floatCat
	case sql.Datetime:
		return dateCat
	case sql.Text, sql.LongText, sql.MediumText, sql.TinyText:
		if _, err := parseDate(l.Value().(string)); err == nil {
			return dateCat
		} else if _, err := literalAsInt64(l); err == nil {
			return intCat
		} else if _, err := literalAsFloat64(l); err == nil {
			return floatCat
		}
	}

	return stringCat
}

type CompareOp interface {
	CompareLiterals(l1, l2 *expression.Literal) (bool, error)
	CompareNomsValues(v1, v2 types.Value) (bool, error)
	CompareToNull(v2 types.Value) (bool, error)
}

type EqualsOp struct{}

func (op EqualsOp) CompareLiterals(l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(l1, l2)

	if err != nil {
		return false, err
	}

	return n == 0, nil
}

func (op EqualsOp) CompareNomsValues(v1, v2 types.Value) (bool, error) {
	return v1.Equals(v2), nil
}

func (op EqualsOp) CompareToNull(v2 types.Value) (bool, error) {
	return types.IsNull(v2), nil
}

type GreaterOp struct {
	NBF *types.NomsBinFormat
}

func (op GreaterOp) CompareLiterals(l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(l1, l2)

	if err != nil {
		return false, err
	}

	return n > 0, nil
}

func (op GreaterOp) CompareNomsValues(v1, v2 types.Value) (bool, error) {
	eq := v1.Equals(v2)

	if eq {
		return false, nil
	}

	lt, err := v1.Less(op.NBF, v2)

	if err != nil {
		return false, nil
	}

	return !lt, err
}

func (op GreaterOp) CompareToNull(v2 types.Value) (bool, error) {
	return false, nil
}

type GreaterEqualOp struct {
	NBF *types.NomsBinFormat
}

func (op GreaterEqualOp) CompareLiterals(l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(l1, l2)

	if err != nil {
		return false, err
	}

	return n >= 0, nil
}

func (op GreaterEqualOp) CompareNomsValues(v1, v2 types.Value) (bool, error) {
	res, err := v1.Less(op.NBF, v2)

	if err != nil {
		return false, err
	}

	return !res, nil
}

func (op GreaterEqualOp) CompareToNull(v2 types.Value) (bool, error) {
	return false, nil
}

type LessOp struct {
	NBF *types.NomsBinFormat
}

func (op LessOp) CompareLiterals(l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(l1, l2)

	if err != nil {
		return false, err
	}

	return n < 0, nil
}

func (op LessOp) CompareNomsValues(v1, v2 types.Value) (bool, error) {
	return v1.Less(op.NBF, v2)
}

func (op LessOp) CompareToNull(v2 types.Value) (bool, error) {
	return false, nil
}

type LessEqualOp struct {
	NBF *types.NomsBinFormat
}

func (op LessEqualOp) CompareLiterals(l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(l1, l2)

	if err != nil {
		return false, err
	}

	return n <= 0, nil
}

func (op LessEqualOp) CompareNomsValues(v1, v2 types.Value) (bool, error) {
	eq := v1.Equals(v2)

	if eq {
		return true, nil
	}

	return v1.Less(op.NBF, v2)
}

func (op LessEqualOp) CompareToNull(v2 types.Value) (bool, error) {
	return false, nil
}
