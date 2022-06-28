// Copyright 2022 Dolthub, Inc.
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

package dfunctions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRevisionDatabaseName(t *testing.T) {
	type testData struct {
		input    string
		dbName   string
		revision string
	}

	tests := []testData{{
		input:    "mydb",
		dbName:   "mydb",
		revision: "",
	}, {
		input:    "mydb",
		dbName:   "mydb",
		revision: "",
	}, {
		input:    "mydb/branch1",
		dbName:   "mydb",
		revision: "branch1",
	}, {
		input:    "mydb/withaslash/branch2",
		dbName:   "mydb/withaslash",
		revision: "branch2",
	}, {
		input:    "",
		dbName:   "",
		revision: "",
	}}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			actualDbName, actualRevision := parseRevisionDatabaseName(test.input)
			require.Equal(t, test.dbName, actualDbName)
			require.Equal(t, test.revision, actualRevision)
		})
	}
}
