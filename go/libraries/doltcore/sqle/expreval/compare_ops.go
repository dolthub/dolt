// Copyright 2020 Dolthub, Inc.
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
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/store/types"
)

func compareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (int, error) {
	return l1.Type().Compare(ctx, l1.Value(), l2.Value())
}

// CompareOp is an interface for comparing values
type CompareOp interface {
	// CompareLiterals compares two go-mysql-server literals
	CompareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (bool, error)
	// CompareNomsValues compares two noms values
	CompareNomsValues(ctx context.Context, v1, v2 types.Value) (bool, error)
	// CompareToNil compares a noms value to nil using sql logic rules
	CompareToNil(v2 types.Value) (bool, error)
}

// EqualsOp implements the CompareOp interface implementing equality logic
type EqualsOp struct{}

// CompareLiterals compares two go-mysql-server literals for equality
func (op EqualsOp) CompareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(ctx, l1, l2)

	if err != nil {
		return false, err
	}

	return n == 0, nil
}

// CompareNomsValues compares two noms values for equality
func (op EqualsOp) CompareNomsValues(_ context.Context, v1, v2 types.Value) (bool, error) {
	return v1.Equals(v2), nil
}

// CompareToNil always returns false as values are neither greater than, less than, or equal to nil
// except for equality op, the compared value is null.
func (op EqualsOp) CompareToNil(v types.Value) (bool, error) {
	if v == types.NullValue {
		return true, nil
	}
	return false, nil
}

// GreaterOp implements the CompareOp interface implementing greater than logic
type GreaterOp struct {
	vr types.ValueReader
}

// CompareLiterals compares two go-mysql-server literals returning true if the value of the first
// is greater than the second.
func (op GreaterOp) CompareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(ctx, l1, l2)

	if err != nil {
		return false, err
	}

	return n > 0, nil
}

// CompareNomsValues compares two noms values returning true if the of the first
// is greater than the second.
func (op GreaterOp) CompareNomsValues(ctx context.Context, v1, v2 types.Value) (bool, error) {
	eq := v1.Equals(v2)

	if eq {
		return false, nil
	}

	lt, err := v1.Less(ctx, op.vr.Format(), v2)

	if err != nil {
		return false, nil
	}

	return !lt, err
}

// CompareToNil always returns false as values are neither greater than, less than, or equal to nil
func (op GreaterOp) CompareToNil(types.Value) (bool, error) {
	return false, nil
}

// GreaterEqualOp implements the CompareOp interface implementing greater than or equal to logic
type GreaterEqualOp struct {
	vr types.ValueReader
}

// CompareLiterals compares two go-mysql-server literals returning true if the value of the first
// is greater than or equal to the second.
func (op GreaterEqualOp) CompareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(ctx, l1, l2)

	if err != nil {
		return false, err
	}

	return n >= 0, nil
}

// CompareNomsValues compares two noms values returning true if the of the first
// is greater or equal to than the second.
func (op GreaterEqualOp) CompareNomsValues(ctx context.Context, v1, v2 types.Value) (bool, error) {
	res, err := v1.Less(ctx, op.vr.Format(), v2)

	if err != nil {
		return false, err
	}

	return !res, nil
}

// CompareToNil always returns false as values are neither greater than, less than, or equal to nil
func (op GreaterEqualOp) CompareToNil(types.Value) (bool, error) {
	return false, nil
}

// LessOp implements the CompareOp interface implementing less than logic
type LessOp struct {
	vr types.ValueReader
}

// CompareLiterals compares two go-mysql-server literals returning true if the value of the first
// is less than the second.
func (op LessOp) CompareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(ctx, l1, l2)

	if err != nil {
		return false, err
	}

	return n < 0, nil
}

// CompareNomsValues compares two noms values returning true if the of the first
// is less than the second.
func (op LessOp) CompareNomsValues(ctx context.Context, v1, v2 types.Value) (bool, error) {
	return v1.Less(ctx, op.vr.Format(), v2)
}

// CompareToNil always returns false as values are neither greater than, less than, or equal to nil
func (op LessOp) CompareToNil(types.Value) (bool, error) {
	return false, nil
}

// LessEqualOp implements the CompareOp interface implementing less than or equal to logic
type LessEqualOp struct {
	vr types.ValueReader
}

// CompareLiterals compares two go-mysql-server literals returning true if the value of the first
// is less than or equal to the second.
func (op LessEqualOp) CompareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (bool, error) {
	n, err := compareLiterals(ctx, l1, l2)

	if err != nil {
		return false, err
	}

	return n <= 0, nil
}

// CompareNomsValues compares two noms values returning true if the of the first
// is less than or equal to the second.
func (op LessEqualOp) CompareNomsValues(ctx context.Context, v1, v2 types.Value) (bool, error) {
	eq := v1.Equals(v2)

	if eq {
		return true, nil
	}

	return v1.Less(ctx, op.vr.Format(), v2)
}

// CompareToNil always returns false as values are neither greater than, less than, or equal to nil
func (op LessEqualOp) CompareToNil(types.Value) (bool, error) {
	return false, nil
}
