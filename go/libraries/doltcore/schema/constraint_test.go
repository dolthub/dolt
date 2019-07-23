// Copyright 2019 Liquidata, Inc.
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
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type TestConstraint struct {
	params map[string]string
}

func (tc TestConstraint) SatisfiesConstraint(value types.Value) bool {
	return true
}

func (tc TestConstraint) GetConstraintType() string {
	return "test"
}

func (tc TestConstraint) GetConstraintParams() map[string]string {
	return tc.params
}

func (tc TestConstraint) String() string {
	return ""
}

func TestColConstraintsAreEqual(t *testing.T) {
	tests := []struct {
		constraints1 []ColConstraint
		constraints2 []ColConstraint
		expectedEq   bool
	}{
		{nil, nil, true},
		{nil, []ColConstraint{}, true},
		{[]ColConstraint{}, []ColConstraint{}, true},
		{[]ColConstraint{NotNullConstraint{}}, []ColConstraint{NotNullConstraint{}}, true},
		{[]ColConstraint{NotNullConstraint{}}, []ColConstraint{TestConstraint{}}, false},
		{[]ColConstraint{TestConstraint{map[string]string{"a": "1", "b": "2"}}}, []ColConstraint{TestConstraint{}}, false},
		{[]ColConstraint{TestConstraint{map[string]string{"a": "1", "b": "2"}}}, []ColConstraint{TestConstraint{map[string]string{"a": "1", "b": "2"}}}, true},
		{[]ColConstraint{}, []ColConstraint{NotNullConstraint{}}, false},
		{nil, []ColConstraint{NotNullConstraint{}}, false},
	}

	for i, test := range tests {
		actualEq := ColConstraintsAreEqual(test.constraints1, test.constraints2)

		if actualEq != test.expectedEq {
			t.Error("test number:", i, "expected equality:", test.expectedEq, "actual equality:", actualEq)
		}
	}
}
