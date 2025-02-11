// Copyright 2025 Dolthub, Inc.
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

package statspro

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type StatsNoop struct{}

func (s StatsNoop) GetTableStats(ctx *sql.Context, db string, table sql.Table) ([]sql.Statistic, error) {
	return nil, nil
}

func (s StatsNoop) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	return nil
}

func (s StatsNoop) SetStats(ctx *sql.Context, stats sql.Statistic) error {
	return nil
}

func (s StatsNoop) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	return nil, false
}

func (s StatsNoop) DropStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) error {
	return nil
}

func (s StatsNoop) DropDbStats(ctx *sql.Context, db string, flush bool) error {
	return nil
}

func (s StatsNoop) RowCount(ctx *sql.Context, db string, table sql.Table) (uint64, error) {
	return 0, nil
}

func (s StatsNoop) DataLength(ctx *sql.Context, db string, table sql.Table) (uint64, error) {
	return 0, nil
}

func (s StatsNoop) CancelRefreshThread(string) {
	return
}

func (s StatsNoop) StartRefreshThread(*sql.Context, dsess.DoltDatabaseProvider, string, *env.DoltEnv, dsess.SqlDatabase) error {
	return nil
}

func (s StatsNoop) ThreadStatus(string) string {
	return "stats disabled"
}

func (s StatsNoop) Prune(ctx *sql.Context) error {
	return nil
}

func (s StatsNoop) Purge(ctx *sql.Context) error {
	return nil
}

func (s StatsNoop) WaitForDbSync(ctx *sql.Context) error {
	return nil
}

var _ sql.StatsProvider = StatsNoop{}
