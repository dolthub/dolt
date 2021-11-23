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

package dsess

import (
	"context"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

type EditSession struct {
	editors map[string]TableEditor
	mu      *sync.RWMutex // This mutex is s
}

func NewEditSession() EditSession {
	return EditSession{
		editors: make(map[string]TableEditor),
		mu:      &sync.RWMutex{},
	}
}

// GetTableEditor returns a TableEditor for the given table. If a schema is provided and it does not match the one
// that is used for currently open editors (if any), then those editors will reload the table from the root.
func (tes EditSession) GetTableEditor(ctx context.Context, sch sql.Schema, tbl *doltdb.Table) (TableEditor, error) {
	panic("unimplemented")
}

// Flush returns an updated root with all of the changed tables.
func (tes EditSession) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	panic("unimplemented")
}

// SetRoot uses the given root to set all open table editors to the state as represented in the root. If any
// tables are removed in the root, but have open table editors, then the references to those are removed. If those
// removed table's editors are used after this, then the behavior is undefined. This will lose any changes that have not
// been flushed. If the purpose is to add a new table, foreign key, etc. (using Flush followed up with SetRoot), then
// use UpdateRoot. Calling the two functions manually for the purposes of root modification may lead to race conditions.
// todo(andy): what is this used for?
func (tes EditSession) SetRoot(ctx context.Context, root *doltdb.RootValue) error {
	panic("unimplemented")
}

// UpdateRoot takes in a function meant to update the root (whether that be updating a table's schema, adding a foreign
// key, etc.) and passes in the flushed root. The function may then safely modify the root, and return the modified root
// (assuming no errors). The TableEditSession will update itself in accordance with the newly returned root.
// todo(andy): what is this used for
func (tes EditSession) UpdateRoot(ctx context.Context, updatingFunc func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error)) error {
	panic("unimplemented")
}
