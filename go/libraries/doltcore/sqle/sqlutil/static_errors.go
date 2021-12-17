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

package sqlutil

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type StaticErrorTable struct {
	sql.Table
	err error
}

func (t *StaticErrorTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return nil, t.err
}

func (t *StaticErrorTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	return nil, t.err
}

func NewStaticErrorTable(orig sql.Table, err error) sql.Table {
	return &StaticErrorTable{orig, err}
}

type StaticErrorRowIter struct {
	err error
}

func NewStaticErrorRowIter(err error) sql.RowIter {
	return &StaticErrorRowIter{err}
}

func (i *StaticErrorRowIter) Next(*sql.Context) (sql.Row, error) {
	return nil, i.err
}

func (i *StaticErrorRowIter) Close(*sql.Context) error {
	// Or i.err?
	return nil
}

type StaticErrorEditor struct {
	err error
}

func NewStaticErrorEditor(err error) *StaticErrorEditor {
	return &StaticErrorEditor{err}
}

func (e *StaticErrorEditor) Insert(*sql.Context, sql.Row) error {
	return e.err
}

func (e *StaticErrorEditor) Delete(*sql.Context, sql.Row) error {
	return e.err
}

func (e *StaticErrorEditor) Update(*sql.Context, sql.Row, sql.Row) error {
	return e.err
}

func (e *StaticErrorEditor) SetAutoIncrementValue(*sql.Context, interface{}) error {
	return e.err
}

func (e *StaticErrorEditor) StatementBegin(ctx *sql.Context) {}

func (e *StaticErrorEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

func (e *StaticErrorEditor) StatementComplete(ctx *sql.Context) error {
	return nil
}

func (e *StaticErrorEditor) Close(*sql.Context) error {
	// Or e.err?
	return nil
}
