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
	return l1.Type().Compare(l1.Value(), l2.Value())
}

type CompareOp interface {
	CompareLiterals(l1, l2 *expression.Literal) (bool, error)
	CompareNomsValues(v1, v2 types.Value) (bool, error)
	CompareToNil(v2 types.Value) (bool, error)
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

func (op EqualsOp) CompareToNil(v2 types.Value) (bool, error) {
	return false, nil
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

func (op GreaterOp) CompareToNil(v2 types.Value) (bool, error) {
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

func (op GreaterEqualOp) CompareToNil(v2 types.Value) (bool, error) {
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

func (op LessOp) CompareToNil(v2 types.Value) (bool, error) {
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

func (op LessEqualOp) CompareToNil(v2 types.Value) (bool, error) {
	return false, nil
}
