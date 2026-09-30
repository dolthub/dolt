// Copyright 2024 Dolthub, Inc.
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

package binlogreplication

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// NewBinlogDropDatabaseHook returns a new DropDatabaseHook function that records a database drop in the binlog events.
func NewBinlogDropDatabaseHook(_ context.Context, listeners []doltdb.DatabaseUpdateListener) func(ctx *sql.Context, name string) {
	return func(ctx *sql.Context, name string) {
		for _, listener := range listeners {
			err := listener.DatabaseDropped(ctx, name)
			if err != nil {
				logrus.Errorf("error notifying working root listener of dropped database: %s", err.Error())
			}
		}
	}
}
