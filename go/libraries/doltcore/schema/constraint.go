// Copyright 2019 Dolthub, Inc.
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

package schema

import (
	"fmt"

	"github.com/dolthub/dolt/go/store/types"
)

// ColConstraint is an interface used for evaluating whether a columns value is valid
// TODO: there is only a single constraint type: NotNull. It should probably just be a bool field on the column.
type ColConstraint interface {
	// GetConstraintType returns a string representation of the type of constraint.  This is used for serialization and
	// deserialization of constraints (see ColConstraintFromTypeAndParams).
	GetConstraintType() string

	// Stringer results are used to inform users of the constraint's properties.
	fmt.Stringer
}

const (
	NotNullConstraintType = "not_null"
)

// NotNullConstraint validates that a value is not null.  It does not restrict 0 length strings, or 0 valued ints, or
// anything other than non nil values
type NotNullConstraint struct{}

// SatisfiesConstraint returns true if value is not nil and not types.NullValue
func (nnc NotNullConstraint) SatisfiesConstraint(value types.Value) bool {
	return !types.IsNull(value)
}

// GetConstraintType returns "not_null"
func (nnc NotNullConstraint) GetConstraintType() string {
	return NotNullConstraintType
}

// String returns a useful description of the constraint
func (nnc NotNullConstraint) String() string {
	return "Not null"
}

// IndexOfConstraint returns the index in the supplied slice of the first constraint of matching type.  If none are
// found then -1 is returned
func IndexOfConstraint(constraints []ColConstraint, constraintType string) int {
	for i, c := range constraints {
		if c.GetConstraintType() == constraintType {
			return i
		}
	}

	return -1
}

// ColConstraintsAreEqual validates two ColConstraint slices are identical.
func ColConstraintsAreEqual(a, b []ColConstraint) bool {
	if len(a) != len(b) {
		return false
	} else if len(a) == 0 {
		return true
	}

	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]

		if ca.GetConstraintType() != cb.GetConstraintType() {
			return false
		}
	}

	return true
}
