// Copyright 2020 Dolthub, Inc.
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

package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
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
			result, err := dconfig.ParseDate(test.dateStr)

			if test.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, result, test.expTime)
			}
		})
	}
}

func TestParsePerson(t *testing.T) {
	tests := []struct {
		personStr string
		personType string
		expName   string
		expEmail  string
		expErr    bool
	}{
		{"Hi <hi@hi.com>", "author", "Hi", "hi@hi.com", false},
		{"John Doe <hi@hi.com>", "author", "John Doe", "hi@hi.com", false},
		{"John Doe <hi@hi.com", "committer", "John Doe", "hi@hi.com", false},
		{"John Doe", "author", "", "", true},
		{"<hi@hi.com>", "committer", "", "", true},
		{"", "author", "", "", true},
		{"John Doe hi@hi.com", "committer", "", "", true},
	}

	for _, test := range tests {
		t.Run(test.personStr+"_"+test.personType, func(t *testing.T) {
			name, email, err := ParsePerson(test.personStr, test.personType)

			if test.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, name, test.expName)
				assert.Equal(t, email, test.expEmail)
			}
		})
	}
}
