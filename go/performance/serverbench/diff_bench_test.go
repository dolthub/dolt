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

package serverbench

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
)

func init() {
	connStr := fmt.Sprintf("%v:%v@tcp(%v:%v)/%s",
		"root", "", "127.0.0.1", 3306, "diffbench")

	conn, err := dbr.Open("mysql", connStr, nil)
	if err != nil {
		panic(err)
	}
	sess = conn.NewSession(&dbr.NullEventReceiver{})
}

var sess *dbr.Session

func BenchmarkServerDiff(b *testing.B) {
	b.Run("point diff", func(b *testing.B) {
		benchmarkQuery(b, "SELECT count(*) "+
			"FROM dolt_commit_diff_difftbl "+
			"WHERE to_commit=HASHOF('HEAD') "+
			"AND from_commit=HASHOF('HEAD^')")
	})
	b.Run("point lookup", func(b *testing.B) {
		benchmarkQuery(b, "SELECT * FROM difftbl WHERE pk = 12345")
	})
}

func benchmarkQuery(b *testing.B, query string) {
	for i := 0; i < b.N; i++ {
		r, err := sess.Query(query)
		require.NoError(b, err)
		require.NoError(b, r.Close())
	}
}
