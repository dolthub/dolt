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

package setalgebra

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/types"
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
