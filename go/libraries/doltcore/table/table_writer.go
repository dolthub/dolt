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

package table

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// TableWriteCloser is an interface for writing rows to a table
type TableWriter interface {
	// GetSchema gets the schema of the rows that this writer writes
	GetSchema() schema.Schema

	// WriteRow will write a row to a table
	WriteRow(ctx context.Context, r row.Row) error
}

// TableWriteCloser is an interface for writing rows to a table, that can be closed
type TableWriteCloser interface {
	TableWriter
	TableCloser
}
