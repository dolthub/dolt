// Copyright 2021 Dolthub, Inc.
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

package altertests

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

type ModifyTypeTest struct {
	FromType     string
	ToType       string
	InsertValues string
	SelectRes    []interface{}
	ExpectedErr  bool
}

func RunModifyTypeTests(t *testing.T, tests []ModifyTypeTest) {
	for _, test := range tests {
		name := fmt.Sprintf("%s -> %s: %s", test.FromType, test.ToType, test.InsertValues)
		if len(name) > 200 {
			name = name[:200]
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dEnv := dtestutils.CreateTestEnv()
			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)
			root, err = executeModify(t, ctx, dEnv, root, fmt.Sprintf("CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 %s);", test.FromType))
			require.NoError(t, err)
			root, err = executeModify(t, ctx, dEnv, root, fmt.Sprintf("INSERT INTO test VALUES %s;", test.InsertValues))
			require.NoError(t, err)
			root, err = executeModify(t, ctx, dEnv, root, fmt.Sprintf("ALTER TABLE test MODIFY v1 %s;", test.ToType))
			if test.ExpectedErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			res, err := executeSelect(t, ctx, dEnv, root, "SELECT v1 FROM test ORDER BY pk;")
			require.NoError(t, err)
			assert.Equal(t, test.SelectRes, res)
		})
	}
}

func SkipByDefaultInCI(t *testing.T) {
	if os.Getenv("CI") != "" && os.Getenv("DOLT_TEST_RUN_NON_RACE_TESTS") == "" {
		t.Skip()
	}
}

func widenValue(v interface{}) interface{} {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	case float32:
		return float64(x)
	default:
		return v
	}
}

func parseTime(timestampLayout bool, value string) time.Time {
	var t time.Time
	var err error
	if timestampLayout {
		t, err = time.Parse("2006-01-02 15:04:05.999999", value)
	} else {
		t, err = time.Parse("2006-01-02", value)
	}
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func executeSelect(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query string) ([]interface{}, error) {
	var err error
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := sqle.NewDatabase("dolt", dEnv.DbData(), opts)
	engine, sqlCtx, err := sqle.NewTestEngine(t, dEnv, ctx, db, root)
	if err != nil {
		return nil, err
	}
	_, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}
	var vals []interface{}
	var r sql.Row
	for r, err = iter.Next(sqlCtx); err == nil; r, err = iter.Next(sqlCtx) {
		if len(r) == 1 {
			// widen the values since we're testing values rather than types
			vals = append(vals, widenValue(r[0]))
		} else if len(r) > 1 {
			return nil, fmt.Errorf("expected return of single value from select: %q", query)
		} else { // no values
			vals = append(vals, nil)
		}
	}
	if err != io.EOF {
		return nil, err
	}
	return vals, nil
}

func executeModify(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, query string) (*doltdb.RootValue, error) {
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	db := sqle.NewDatabase("dolt", dEnv.DbData(), opts)
	engine, sqlCtx, err := sqle.NewTestEngine(t, dEnv, ctx, db, root)
	if err != nil {
		return nil, err
	}
	_, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}
	for {
		_, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	err = iter.Close(sqlCtx)
	if err != nil {
		return nil, err
	}
	return db.GetRoot(sqlCtx)
}
