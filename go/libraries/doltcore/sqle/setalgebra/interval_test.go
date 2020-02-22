package setalgebra

import (
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func testInterv(start, end *IntervalEndpoint) Interval {
	return Interval{types.Format_Default, start, end}
}

func TestCompareIntervals(t *testing.T) {
	tests := []struct {
		name        string
		in1         Interval
		in2         Interval
		expected    intervalComparison
		revExpected intervalComparison
		expectErr   bool
	}{
		{
			"universal compare",
			testInterv(nil, nil),
			testInterv(nil, nil),
			intervalComparison{0, -1, 1, 0},
			intervalComparison{0, -1, 1, 0},
			false,
		},

		{
			"negative and positive infinite intervals meeting at open endpoint",
			testInterv(nil, &IntervalEndpoint{types.Int(0), false}),
			testInterv(&IntervalEndpoint{types.Int(0), false}, nil),
			intervalComparison{-1, -1, -1, -1},
			intervalComparison{1, 1, 1, 1},
			false,
		},

		{
			"negative and positive infinite intervals meeting at closed endpoint",
			testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
			testInterv(&IntervalEndpoint{types.Int(0), true}, nil),
			intervalComparison{-1, -1, 0, -1},
			intervalComparison{1, 0, 1, 1},
			false,
		},

		{
			"overlapping",
			testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(6), true}),
			testInterv(&IntervalEndpoint{types.Int(0), false}, &IntervalEndpoint{types.Int(3), true}),
			intervalComparison{-1, -1, 1, 1},
			intervalComparison{1, -1, 1, -1},
			false,
		},

		{
			"negative and positive infinite intervals meeting at open endpoint",
			testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(1), true}),
			testInterv(&IntervalEndpoint{types.Int(2), true}, &IntervalEndpoint{types.Int(3), true}),
			intervalComparison{-1, -1, -1, -1},
			intervalComparison{1, 1, 1, 1},
			false,
		},

		{
			"equal open starting point",
			testInterv(&IntervalEndpoint{types.Int(0), false}, &IntervalEndpoint{types.Int(1), true}),
			testInterv(&IntervalEndpoint{types.Int(0), false}, &IntervalEndpoint{types.Int(3), true}),
			intervalComparison{0, -1, 1, -1},
			intervalComparison{0, -1, 1, 1},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := compareIntervals(test.in1, test.in2)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, res, test.expected)

			res, err = compareIntervals(test.in2, test.in1)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, res, test.revExpected)
		})
	}
}
