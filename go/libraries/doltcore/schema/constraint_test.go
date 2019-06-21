package schema

import (
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
	"testing"
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
