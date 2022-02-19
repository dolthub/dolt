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

package memprof

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var loc = flag.String("doltDir", "", "Directory of dolt database")
var urlStr string
var ddb *doltdb.DoltDB

func TestMain(m *testing.M) {
	flag.Parse()

	urlStr = "file://" + *loc + dbfactory.DoltDataDir

	code := m.Run()
	os.Exit(code)
}

func BenchmarkLoadDoltDBMemory(b *testing.B) {
	b.SkipNow()
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		var err error
		ddb, err = doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr, filesys.LocalFS)
		if err != nil {
			b.Fatalf("failed to load doltdb, err: %s", err.Error())
		}
	}
}
