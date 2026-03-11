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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func compareLiterals(ctx *sql.Context, l1, l2 *expression.Literal) (int, error) {
	return l1.Type().Compare(ctx, l1.Value(), l2.Value())
}

// CompareOp is an interface for comparing values using SQL type semantics.
type CompareOp interface {
	// ApplyCmp applies the comparison operator to the result of sql.Type.Compare,
	// returning true if the comparison satisfies the operator.
	ApplyCmp(n int) bool
	// CompareToNil returns the comparison result when the column value is null.
	// otherIsNull indicates whether the value being compared to is also null.
	CompareToNil(otherIsNull bool) bool
}

// EqualsOp implements the CompareOp interface implementing equality logic
type EqualsOp struct{}

// ApplyCmp implements CompareOp, returning true when the values are equal.
func (op EqualsOp) ApplyCmp(n int) bool { return n == 0 }

// CompareToNil implements CompareOp, returning true only when both values are null.
func (op EqualsOp) CompareToNil(otherIsNull bool) bool { return otherIsNull }

// GreaterOp implements the CompareOp interface implementing greater than logic
type GreaterOp struct{}

// ApplyCmp implements CompareOp, returning true when the first value is greater than the second.
func (op GreaterOp) ApplyCmp(n int) bool { return n > 0 }

// CompareToNil implements CompareOp, always returning false as null comparisons are never greater than.
func (op GreaterOp) CompareToNil(bool) bool { return false }

// GreaterEqualOp implements the CompareOp interface implementing greater than or equal to logic
type GreaterEqualOp struct{}

// ApplyCmp implements CompareOp, returning true when the first value is greater than or equal to the second.
func (op GreaterEqualOp) ApplyCmp(n int) bool { return n >= 0 }

// CompareToNil implements CompareOp, always returning false as null comparisons are never greater than or equal.
func (op GreaterEqualOp) CompareToNil(bool) bool { return false }

// LessOp implements the CompareOp interface implementing less than logic
type LessOp struct{}

// ApplyCmp implements CompareOp, returning true when the first value is less than the second.
func (op LessOp) ApplyCmp(n int) bool { return n < 0 }

// CompareToNil implements CompareOp, always returning false as null comparisons are never less than.
func (op LessOp) CompareToNil(bool) bool { return false }

// LessEqualOp implements the CompareOp interface implementing less than or equal to logic
type LessEqualOp struct{}

// ApplyCmp implements CompareOp, returning true when the first value is less than or equal to the second.
func (op LessEqualOp) ApplyCmp(n int) bool { return n <= 0 }

// CompareToNil implements CompareOp, always returning false as null comparisons are never less than or equal.
func (op LessEqualOp) CompareToNil(bool) bool { return false }
