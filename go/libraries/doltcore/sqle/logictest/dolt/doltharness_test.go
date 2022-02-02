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

package dolt

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func createTestEnvWithFS(fs filesys.Filesys, workingDir string) *env.DoltEnv {
	testHomeDirFunc := func() (string, error) { return workingDir, nil }
	const name = "test mcgibbins"
	const email = "bigfakeytester@fake.horse"
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB, "test")
	err := dEnv.InitRepo(context.Background(), types.Format_Default, name, email, env.DefaultInitBranch)
	if err != nil {
		panic("Failed to initialize environment")
	}
	return dEnv
}

type statementTest struct {
	statement string
	expErr    error
}

type queryTest struct {
	query      string
	expErr     error
	expSchema  string
	expResults []string
}

func TestDoltHarness(t *testing.T) {
	tmp := "/doesnotexist/tmp"
	wd := filepath.Join(tmp, "/harnesstester")

	statementTests := []statementTest{
		{
			statement: "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);",
			expErr:    nil,
		},
		{
			statement: "INSERT INTO t1(e,c,b,d,a) VALUES(NULL,102,NULL,101,104);",
			expErr:    nil,
		},
		{
			statement: "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105);",
			expErr:    nil,
		},
	}

	queryTests := []queryTest{
		{
			query:      "SELECT a,c,e FROM t1;",
			expErr:     nil,
			expSchema:  "III",
			expResults: []string{"104", "102", "NULL", "107", "106", "109"},
		},
		{
			query:      "SELECT b,d FROM t1;",
			expErr:     nil,
			expSchema:  "II",
			expResults: []string{"NULL", "101", "105", "108"},
		},
		{
			query:      "SELECT * FROM t1 WHERE d < 107;",
			expErr:     nil,
			expSchema:  "IIIII",
			expResults: []string{"104", "NULL", "102", "101", "NULL"},
		},
		{
			query:      "SELECT * FROM t1 WHERE d > 102;",
			expErr:     nil,
			expSchema:  "IIIII",
			expResults: []string{"107", "105", "106", "108", "109"},
		},
	}

	fs := filesys.NewInMemFS([]string{}, nil, tmp)
	dEnv := createTestEnvWithFS(fs, wd)

	// We run this several times in a row to make sure that the same dolt env can be used in multiple setup / teardown
	// cycles
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("dolt harness runner %d", i), func(t *testing.T) {
			h := &DoltHarness{}
			err := innerInit(h, dEnv)
			assert.Equal(t, nil, err)

			// setup repo with statements
			for _, test := range statementTests {
				err = h.ExecuteStatement(test.statement)
				assert.Equal(t, test.expErr, err)
			}

			// test queries
			for _, test := range queryTests {
				schema, results, err := h.ExecuteQuery(test.query)
				assert.Equal(t, test.expErr, err)
				assert.Equal(t, test.expSchema, schema)
				assert.Equal(t, test.expResults, results)
			}
		})
	}
}
