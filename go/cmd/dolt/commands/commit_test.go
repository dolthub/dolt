// Copyright 2019 Dolthub, Inc.
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

package commands

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDate(t *testing.T) {
	tests := []struct {
		dateStr string
		expTime time.Time
		expErr  bool
	}{
		{"1901/09/30", time.Date(1901, 9, 30, 0, 0, 0, 0, time.UTC), false},
		{"2019/01/20", time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), false},
		{"2019-1-20", time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), true},
		{"2019.01.20", time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), false},
		{"2019/01/20T13:49:59", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), false},
		{"2019-01-20T13:49:59", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), false},
		{"2019.01.20T13:49:59", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), false},
		{"2019.01.20T13:49", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), true},
		{"2019.01.20T13", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), true},
		{"2019.01", time.Date(2019, 1, 20, 13, 49, 59, 0, time.UTC), true},
	}

	for _, test := range tests {
		t.Run(test.dateStr, func(t *testing.T) {
			result, err := ParseDate(test.dateStr)

			if test.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, result, test.expTime)
			}
		})
	}
}

func TestParseAuthor(t *testing.T) {
	tests := []struct {
		authorStr string
		expName   string
		expEmail  string
		expErr    bool
	}{
		{"Hi <hi@hi.com>", "Hi", "hi@hi.com", false},
		{"John Doe <hi@hi.com>", "John Doe", "hi@hi.com", false},
		{"John Doe <hi@hi.com", "John Doe", "hi@hi.com", false},
		{"John Doe", "", "", true},
		{"<hi@hi.com>", "", "", true},
		{"", "", "", true},
		{"John Doe hi@hi.com", "", "", true},
	}

	for _, test := range tests {
		t.Run(test.authorStr, func(t *testing.T) {
			author, email, err := parseAuthor(test.authorStr)

			if test.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, author, test.expName)
				assert.Equal(t, email, test.expEmail)
			}
		})
	}
}
