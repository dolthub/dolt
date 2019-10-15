package schcmds

import (
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"testing"
)

func TestLeastPermissiveKind(t *testing.T) {
	tests := []struct {
		valStr string
		expKind types.NomsKind
	} {
		{"00000000-0000-0000-0000-000000000000", types.UUIDKind},
		{"00000000-0000-0000-0000-00000000000z", types.StringKind},
		{"0.0000", types.FloatKind},
	}

	for _, test := range tests {
		actualKind := leastPermissiveKind(test.valStr)
		assert.Equal(t, test.expKind, actualKind, "val: %s, expected: %v, actual: %v", test.valStr, test.expKind, actualKind)
	}
}

func TestLeastPermissiveNumericKind(t *testing.T) {
	var maxIntPlusTwo uint64 = 1 << 63 + 1
	tests := []struct {
		name string
		valStr string
		expKind types.NomsKind
		expNegative bool
	} {
		{"empty string", "", types.NullKind, false},
		{"zero", "0", types.IntKind, false},
		{"zero float", "0.0", types.FloatKind, false},
		{"negative float", "-1.3451234", types.FloatKind, true},
		{"double decimal point", "0.00.0", types.NullKind, false},
		{"zero float with high precision","0.0000", types.FloatKind, false},
		{"all zeroes", "0000", types.NullKind, false},
		{"leading zeroes", "01", types.NullKind, false},
		{"negative int", "-1234", types.IntKind, true},
		{"fits in uint64 but not int64", strconv.FormatUint(maxIntPlusTwo, 10), types.UintKind, false},
		{"negative less than math.MinInt64", "-" + strconv.FormatUint(maxIntPlusTwo, 10), types.NullKind, false},
		{"math.MinInt64", strconv.FormatInt(math.MinInt64, 10), types.IntKind, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isNegative, actualKind := leastPermissiveNumericKind(test.valStr)
			assert.Equal(t, test.expKind, actualKind, "val: %s, expected: %v, actual: %v", test.valStr, test.expKind, actualKind)
			assert.Equal(t, test.expNegative, isNegative)
		})
	}
}
