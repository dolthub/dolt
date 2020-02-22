package setalgebra

import (
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func assertErr(t *testing.T, expectErr bool, err error) {
	if expectErr {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

func mustFiniteSet(fs FiniteSet, err error) FiniteSet {
	if err != nil {
		panic(err)
	}

	return fs
}

func TestFiniteSetUnion(t *testing.T) {
	tests := []struct {
		name         string
		setVals1     []types.Value
		setVals2     []types.Value
		expectedVals []types.Value
		expectErr    bool
	}{
		{
			"empty finite set",
			[]types.Value{},
			[]types.Value{},
			[]types.Value{},
			false,
		},
		{
			"non overlapping",
			[]types.Value{types.Int(1), types.Int(2)},
			[]types.Value{types.Int(3), types.Int(4)},
			[]types.Value{types.Int(1), types.Int(2), types.Int(3), types.Int(4)},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs1, err := NewFiniteSet(types.Format_Default, test.setVals1...)
			require.NoError(t, err)

			fs2, err := NewFiniteSet(types.Format_Default, test.setVals2...)
			require.NoError(t, err)

			res, err := finiteSetUnion(fs1, fs2)
			assertErr(t, test.expectErr, err)

			expected, err := NewFiniteSet(types.Format_Default, test.expectedVals...)
			require.NoError(t, err)

			assert.Equal(t, expected, res)
		})
	}
}

func TestFiniteSetIntervalUnion(t *testing.T) {
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
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), true},
			},
			false,
		},
		{
			"some of set outside interval",
			[]types.Value{types.Int(0), types.Int(5), types.Int(20)},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), true},
			},
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
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(10), true},
			},
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
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			false,
		},
		{
			"positive infinity",
			[]types.Value{types.Int(0), types.Int(5), types.Int(10)},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				nil,
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				nil,
			},
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
			Interval{
				types.Format_Default,
				nil,
				nil,
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs, err := NewFiniteSet(types.Format_Default, test.setVals...)
			require.NoError(t, err)

			res, err := finiteSetIntervalUnion(fs, test.in)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestFiniteSetCompositeSetUnion(t *testing.T) {
	tests := []struct {
		name      string
		setVals   []types.Value
		cs        CompositeSet
		expected  Set
		expectErr bool
	}{
		{
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
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(-10), types.Int(20), types.Int(30))),
				[]Interval{
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(0), true},
						&IntervalEndpoint{types.Int(10), true},
					},
				},
			},
			false,
		},
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
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(-10), types.Int(20), types.Int(30), types.Int(99))),
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
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs, err := NewFiniteSet(types.Format_Default, test.setVals...)
			require.NoError(t, err)

			res, err := finiteSetCompositeSetUnion(fs, test.cs)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestIntervalUnion(t *testing.T) {
	tests := []struct {
		name      string
		in1       Interval
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
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(0), true},
						&IntervalEndpoint{types.Int(10), false},
					},
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(10), false},
						&IntervalEndpoint{types.Int(20), true},
					},
				},
			},
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
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			false,
		},
		{
			"fully enclose interval",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(5), true},
				&IntervalEndpoint{types.Int(15), true},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(0), true},
				&IntervalEndpoint{types.Int(20), true},
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
				&IntervalEndpoint{types.Int(0), true},
				nil,
			},
			false,
		},
		{
			"No overlap",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				nil,
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(0), true},
			},
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{
					{
						types.Format_Default,
						nil,
						&IntervalEndpoint{types.Int(0), true},
					},
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(10), true},
						nil,
					},
				},
			},
			false,
		},
		{
			"Overlap at closed endpoint",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				nil,
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			UniversalSet{},
			false,
		},
		{
			"overlapping at open endpoint",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), false},
				nil,
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{
					{
						types.Format_Default,
						nil,
						&IntervalEndpoint{types.Int(10), false},
					},
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(10), false},
						nil,
					},
				},
			},
			false,
		},
		{
			"finite interval start equal to infinite interval end",
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				&IntervalEndpoint{types.Int(20), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(20), false},
			},
			false,
		},
		{
			"finite interval start equal to infinite interval end 2",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				&IntervalEndpoint{types.Int(20), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(20), false},
			},
			false,
		},
		{
			"finite interval start and end less than infinite end",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				&IntervalEndpoint{types.Int(15), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(20), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(20), false},
			},
			false,
		},

		{
			"infinite interval end between finite interval start and end",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(10), true},
				&IntervalEndpoint{types.Int(25), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(20), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(25), false},
			},
			false,
		},
		{
			"no overlap between finite interval and infinite interval",
			Interval{
				types.Format_Default,
				&IntervalEndpoint{types.Int(20), false},
				&IntervalEndpoint{types.Int(30), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{
					{
						types.Format_Default,
						nil,
						&IntervalEndpoint{types.Int(10), false},
					},
					{
						types.Format_Default,
						&IntervalEndpoint{types.Int(20), false},
						&IntervalEndpoint{types.Int(30), false},
					},
				},
			},
			false,
		},
		{
			"in1 < in2",
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(5), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			false,
		},
		{
			"in1 > in2",
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(5), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			false,
		},

		{
			"in1 == in2, in1 inclusive",
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			false,
		},

		{
			"in1 == in2, in2 inclusive",
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), false},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			Interval{
				types.Format_Default,
				nil,
				&IntervalEndpoint{types.Int(10), true},
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := intervalUnion(test.in1, test.in2)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestIntervalCompositeSetUnion(t *testing.T) {
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
			UniversalSet{},
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
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
					testInterv(&IntervalEndpoint{types.Int(15), true}, &IntervalEndpoint{types.Int(30), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			false,
		},

		{
			"Join two intervals",
			testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(25))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(20), false}),
					testInterv(&IntervalEndpoint{types.Int(30), false}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(100), true}),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := intervalCompositeSetUnion(test.in, test.cs)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, test.expected, res)
		})
	}
}

func TestCompositeSetUnion(t *testing.T) {
	tests := []struct {
		name      string
		cs1       CompositeSet
		cs2       CompositeSet
		expected  Set
		expectErr bool
	}{
		{
			"make universal set",
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
			UniversalSet{},
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
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(10))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
					testInterv(&IntervalEndpoint{types.Int(15), true}, &IntervalEndpoint{types.Int(30), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			false,
		},

		{
			"Join two intervals",
			CompositeSet{
				FiniteSet{make(map[hash.Hash]types.Value)},
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(20), true}, &IntervalEndpoint{types.Int(30), true}),
				},
			},
			CompositeSet{
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(25))),
				[]Interval{
					testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(20), false}),
					testInterv(&IntervalEndpoint{types.Int(30), false}, &IntervalEndpoint{types.Int(100), true}),
				},
			},
			testInterv(&IntervalEndpoint{types.Int(0), true}, &IntervalEndpoint{types.Int(100), true}),
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
				mustFiniteSet(NewFiniteSet(types.Format_Default, types.Int(1), types.Int(10), types.Int(400))),
				[]Interval{
					testInterv(nil, &IntervalEndpoint{types.Int(0), true}),
					testInterv(&IntervalEndpoint{types.Int(15), true}, &IntervalEndpoint{types.Int(30), true}),
					testInterv(&IntervalEndpoint{types.Int(50), true}, &IntervalEndpoint{types.Int(100), true}),
					testInterv(&IntervalEndpoint{types.Int(200), true}, &IntervalEndpoint{types.Int(300), true}),
				},
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := compositeUnion(test.cs1, test.cs2)
			assertErr(t, test.expectErr, err)

			assert.Equal(t, test.expected, res)
		})
	}

}
