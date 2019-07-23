package valutil

import (
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestNilSafeEqCheck(t *testing.T) {
	tests := []struct {
		v1         types.Value
		v2         types.Value
		expectedEq bool
	}{
		{nil, nil, true},
		{nil, types.NullValue, true},
		{nil, types.String("blah"), false},
		{types.NullValue, types.String("blah"), false},
		{types.String("blah"), types.String("blah"), true},
	}

	for i, test := range tests {
		actual := NilSafeEqCheck(test.v1, test.v2)

		if actual != test.expectedEq {
			t.Error("test", i, "failed")
		}
	}
}
