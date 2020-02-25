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
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestFiniteSetIntersection(t *testing.T) {
	tests := []struct {
		name      string
		setVals1  []types.Value
		setVals2  []types.Value
		expected  Set
		expectErr bool
	}{
		{
			"empty finite set",
			[]types.Value{},
			[]types.Value{},
			EmptySet{},
			false,
		},
		{
			"non overlapping",
			[]types.Value{types.Int(1), types.Int(2)},
			[]types.Value{types.Int(3), types.Int(4)},
			EmptySet{},
			false,
		},
		{
			"some overlapping",
			[]types.Value{types.Int(1), types.Int(2)},
			[]types.Value{types.Int(2), types.Int(3)},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(2))),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs1, err := NewFiniteSet(types.Format_Default, test.setVals1...)
			require.NoError(t, err)

			fs2, err := NewFiniteSet(types.Format_Default, test.setVals2...)
			require.NoError(t, err)

			res, err := finiteSetIntersection(fs1, fs2)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestFiniteSetIntervalIntersection(t *testing.T) {
	tests := []struct {
		name      string
		setVals   []types.Value
		in        Interval
		expected  Set
		expectErr bool
	}{
		{
			"interval contains set",
			[]types.Value{types.Int(0), types.Int(5), types.Int(10)},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), true},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(5), types.Int(10))),
			false,
		},
		{
			"some of set outside interval",
			[]types.Value{types.Int(-5), types.Int(0), types.Int(5), types.Int(20)},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), true},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(5))),
			false,
		},
		{
			"open intervals",
			[]types.Value{types.Int(0), types.Int(5), types.Int(10)},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), false},
				&IntervalEndpoint{types.Int(10), false},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(5))),
			false,
		},
		{
			"negative infinity",
			[]types.Value{types.Int(0), types.Int(5), types.Int(10)},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(5), types.Int(10))),
			false,
		},
		{
			"positive infinity",
			[]types.Value{types.Int(0), types.Int(5), types.Int(10)},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), false},
				nil,
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(5), types.Int(10))),
			false,
		},
		{
			"negative to positive infinity",
			[]types.Value{types.Int(0), types.Int(5), types.Int(10)},
			Interval{
				types.Format_Default,
				nil,
				nil,
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(5), types.Int(10))),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs, err := NewFiniteSet(types.Format_Default, test.setVals...)
			require.NoError(t, err)

			res, err := finiteSetIntervalIntersection(fs, test.in)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestFiniteSetCompositeSetIntersection(t *testing.T) {
	tests := []struct {
		name      string
		setVals   []types.Value
		cs        CompositeSet
		expected  Set
		expectErr bool
	}{
		/*{
			"empty set",
			[]types.Value{},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(20))),
				[]Interval{
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(0), true},
						&IntervalEndpoint{types.Int(10), true},
					},
				},
			},
			EmptySet{},
			false,
		},
		{
			"set with contained and uncontained vals",
			[]types.Value{types.Int(-10), types.Int(0), types.Int(10), types.Int(20), types.Int(30)},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(20))),
				[]Interval{
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(0), true},
						&IntervalEndpoint{types.Int(10), true},
					},
				},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(10), types.Int(20))),
			false,
		},*/
		{
			"composite set with multiple intervals",
			[]types.Value{types.Int(-10), types.Int(0), types.Int(10), types.Int(20), types.Int(30), types.Int(99), types.Int(100), types.Int(0x7FFFFFFFFFFFFFFF)},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(20))),
				[]Interval{
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(0), true},
						&IntervalEndpoint{types.Int(10), true},
					},
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(100), true},
						nil,
					},
				},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(10), types.Int(20), types.Int(100), types.Int(0x7FFFFFFFFFFFFFFF))),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs, err := NewFiniteSet(types.Format_Default, test.setVals...)
			require.NoError(t, err)

			res, err := finiteSetCompositeSetIntersection(fs, test.cs)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestIntersectIntervalWithStartPoints(t *testing.T) {
	tests := []struct {
		name      string
		in        Interval
		in2       Interval
		expected  Set
		expectErr bool
	}{
		{
			"No overlap",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), false},
				&IntervalEndpoint{types.Int(20), true},
			},
			EmptySet{},
			false,
		},
		{
			"overlap at endpoints",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10))),
			false,
		},
		{
			"1 fully enclosed in 2",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(5), true},
				&IntervalEndpoint{types.Int(15), false},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(5), true},
				&IntervalEndpoint{types.Int(15), false},
			},
			false,
		},
		{
			"2 fully enclosed 1",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(5), true},
				&IntervalEndpoint{types.Int(15), false},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(5), true},
				&IntervalEndpoint{types.Int(15), false},
			},
			false,
		},
		{
			"intersect open and closed endpoints",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), false},
				&IntervalEndpoint{types.Int(20), false},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), false},
				&IntervalEndpoint{types.Int(20), false},
			},
			false,
		},
		{
			"overlap with infinite interval",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				nil,
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := intervalIntersection(test.in, test.in2)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestIntervalCompositeSetIntersection(t *testing.T) {
	tests := []struct {
		name      string
		in        Interval
		cs        CompositeSet
		expected  Set
		expectErr bool
	}{
		{
			"make universal set",
			testInterv(&IntervalEndpoint{types.Int(0), true}, nil),
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10), types.Int(20))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
				},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(10), types.Int(20))),
			false,
		},
		{
			"multiple intervals one overlapping",
			testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10), types.Int(30))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
					testInterv(&IntervalEndpoint{types.Int(15), true}, &IntervalEndpoint{types.Int(25), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(30))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(25), true}),
				},
			},
			false,
		},

		{
			"open interval intersection",
			testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(25))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(20), false}),
					testInterv(&IntervalEndpoint{types.Int(30), false}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(25))),
			false,
		},

		{
			"single interval intersection result",
			testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(35))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(25), false}),
					testInterv(&IntervalEndpoint{types.Int(30), false}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(25), false}),
			false,
		},

		{
			"empty intersection result",
			testInterv(&IntervalEndpoint{types.Int(26), true}, &IntervalEndpoint{types.Int(29), true}),
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(35))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(25), false}),
					testInterv(&IntervalEndpoint{types.Int(30), false}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			EmptySet{},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := intervalCompositeSetIntersection(test.in, test.cs)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestCompositeSetIntersection(t *testing.T) {
	tests := []struct {
		name      string
		cs1       CompositeSet
		cs2       CompositeSet
		expected  Set
		expectErr bool
	}{
		{
			"single point overlap with interval and finiteSet overlap",
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{testInterv(&IntervalEndpoint{types.Int(0), true}, nil)},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10), types.Int(20))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
				},
			},
			mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(0), types.Int(10), types.Int(20))),
			false,
		},
		{
			"multiple intervals one overlapping",
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10), types.Int(30))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
					testInterv(&IntervalEndpoint{types.Int(15), true}, &IntervalEndpoint{types.Int(25), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(30))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(25), true}),
				},
			},
			false,
		},

		{
			"multiple intervals in multiple composite sets",
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10), types.Int(15), types.Int(400))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
					testInterv(&IntervalEndpoint{types.Int(200), true}, &IntervalEndpoint{types.Int(300), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(1), types.Int(10), types.Int(30))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
					testInterv(&IntervalEndpoint{types.Int(15), true}, &IntervalEndpoint{types.Int(25), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10), types.Int(15), types.Int(30))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(25), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			false,
		},

		{
			"No overlap",
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(-10))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(50))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(10), false}),
				},
			},
			EmptySet{},
			false,
		},

		{
			"Single interval result",
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(-10))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(50))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(25), false}),
				},
			},
			testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(25), false}),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := compositeIntersection(test.cs1, test.cs2)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, test.expected, res)
		})
	}

}
