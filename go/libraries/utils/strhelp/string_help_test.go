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

package strhelp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestCommaIfy(t *testing.T) {
	tests := map[int64]string{
		1:        "1",
		10:       "10",
		100:      "100",
		1000:     "1,000",
		10000:    "10,000",
		100000:   "100,000",
		1000000:  "1,000,000",
		10000000: "10,000,000",
	}

	for i, expected := range tests {
		str := CommaIfy(i)
		assert.Equal(t, expected, str)
	}
}
