// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
)

type staticErrorTable struct {
	sql.Table
	err error
}

func (t *staticErrorTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return nil, t.err
}

func (t *staticErrorTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	return nil, t.err
}

func newStaticErrorTable(orig sql.Table, err error) sql.Table {
	return &staticErrorTable{orig, err}
}

type staticErrorRowIter struct {
	err error
}

func newStaticErrorRowIter(err error) sql.RowIter {
	return &staticErrorRowIter{err}
}

func (i *staticErrorRowIter) Next() (sql.Row, error) {
	return nil, i.err
}

func (i *staticErrorRowIter) Close() error {
	// Or i.err?
	return nil
}

type staticErrorEditor struct {
	err error
}

func newStaticErrorEditor(err error) *staticErrorEditor {
	return &staticErrorEditor{err}
}

func (e *staticErrorEditor) Insert(*sql.Context, sql.Row) error {
	return e.err
}

func (e *staticErrorEditor) Delete(*sql.Context, sql.Row) error {
	return e.err
}

func (e *staticErrorEditor) Update(*sql.Context, sql.Row, sql.Row) error {
	return e.err
}

func (e *staticErrorEditor) Close(*sql.Context) error {
	// Or e.err?
	return nil
}
