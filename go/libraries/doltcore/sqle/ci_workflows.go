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

package sqle

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.StatisticsTable = (*WorkflowsTable)(nil)
var _ sql.Table = (*WorkflowsTable)(nil)
var _ dtables.VersionableTable = (*WorkflowsTable)(nil)

// WorkflowsTable is a sql.Table implementation that implements a system table which stores dolt CI workflows
type WorkflowsTable struct {
	*WritableDoltTable
}

// NewWorkflowsTable creates a WorkflowsTable
func NewWorkflowsTable(_ *sql.Context, backingTable *WritableDoltTable) sql.Table {
	return &WorkflowsTable{backingTable}
}

// NewEmptyWorkflowsTable creates a WorkflowsTable
func NewEmptyWorkflowsTable(_ *sql.Context) sql.Table {
	return &WorkflowsTable{&WritableDoltTable{}}
}
