package strhelp

import "testing"

func TestNthTokenTest(t *testing.T) {
	tests := []struct {
		in          string
		n           int
		expectedStr string
		expectedOk  bool
	}{
		{
			"",
			0,
			"",
			true,
		},
		{
			"",
			1,
			"",
			false,
		},
		{
			"short",
			0,
			"short",
			true,
		},
		{
			"short",
			1,
			"",
			false,
		},
		{
			"0/1/2",
			0,
			"0",
			true,
		},
		{
			"0/1/2",
			1,
			"1",
			true,
		},
		{
			"0/1/2",
			2,
			"2",
			true,
		},
		{
			"0/1/2",
			3,
			"",
			false,
		},
		{
			"/1/2/",
			0,
			"",
			true,
		},
		{
			"/1/2/",
			1,
			"1",
			true,
		},
		{
			"/1/2/",
			2,
			"2",
			true,
		},
		{
			"/1/2/",
			3,
			"",
			true,
		},
		{
			"/1/2/",
			4,
			"",
			false,
		},
	}

	for _, test := range tests {
		token, ok := NthToken(test.in, '/', test.n)

		if token != test.expectedStr || ok != test.expectedOk {
			t.Error(test.in, test.n, "th token should be", test.expectedStr, "but it is", token)
		}
	}
}
