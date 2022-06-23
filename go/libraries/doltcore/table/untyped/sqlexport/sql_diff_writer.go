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

package sqlexport

import (
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/go-mysql-server/sql"
)

type SqlDiffWriter struct {
	tableName            string
	sch                  sql.Schema
	writtenFirstRow      bool
	writtenAutocommitOff bool
	writeCloser io.WriteCloser
	editOpts             editor.Options
	autocommitOff        bool
}

func NewSqlDiffWriter(tableName string, schema sql.Schema, wr io.WriteCloser) *SqlDiffWriter {
	return &SqlDiffWriter{
		tableName:            tableName,
		sch:                  schema,
		writtenFirstRow:      false,
		writeCloser: wr,
	}
}
