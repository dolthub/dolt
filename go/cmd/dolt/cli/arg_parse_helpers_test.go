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
			author, email, err := ParseAuthor(test.authorStr)

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

func TestParseBackupPruneGracePeriod(t *testing.T) {
	tests := []struct {
		name     string
		graceStr string
		minEnv   string
		expected time.Duration
		expErr   bool
	}{
		{name: "hours", graceStr: "1h", expected: time.Hour},
		{name: "at the floor", graceStr: "10m", expected: 10 * time.Minute},
		{name: "compound", graceStr: "2h30m", expected: 2*time.Hour + 30*time.Minute},
		{name: "below the floor", graceStr: "5m", expErr: true},
		{name: "zero", graceStr: "0", expErr: true},
		{name: "negative", graceStr: "-1h", expErr: true},
		{name: "not a duration", graceStr: "an hour", expErr: true},
		{name: "bare number", graceStr: "3600", expErr: true},
		{name: "empty", graceStr: "", expErr: true},
		// Tests lower the floor so they do not have to wait it out.
		{name: "floor lowered by env", graceStr: "1s", minEnv: "1s", expected: time.Second},
		{name: "still below a lowered floor", graceStr: "500ms", minEnv: "1s", expErr: true},
		{name: "unparseable env floor", graceStr: "1h", minEnv: "soon", expErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.minEnv != "" {
				t.Setenv(dconfig.EnvBackupPruneMinGrace, test.minEnv)
			}

			grace, err := ParseBackupPruneGracePeriod(test.graceStr)

			if test.expErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expected, grace)
			}
		})
	}
}
