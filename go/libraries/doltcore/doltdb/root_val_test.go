// Copyright 2019 Liquidata, Inc.
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

package doltdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestTableDiff(t *testing.T) {
	ddb, _ := LoadDoltDB(context.Background(), types.Format_7_18, InMemDoltDB)
	ddb.WriteEmptyRepo(context.Background(), "billy bob", "bigbillieb@fake.horse")

	cs, _ := NewCommitSpec("head", "master")
	cm, _ := ddb.Resolve(context.Background(), cs)

	root, err := cm.GetRootValue()
	assert.NoError(t, err)
	added, modified, removed, err := root.TableDiff(context.Background(), root)
	assert.NoError(t, err)

	if len(added)+len(modified)+len(removed) != 0 {
		t.Error("Bad table diff when comparing two repos")
	}

	sch := createTestSchema()
	m, err := types.NewMap(context.Background(), ddb.ValueReadWriter())
	assert.NoError(t, err)

	tbl1, err := createTestTable(ddb.ValueReadWriter(), sch, m)
	assert.NoError(t, err)

	root2, err := root.PutTable(context.Background(), "tbl1", tbl1)
	assert.NoError(t, err)

	added, modified, removed, err = root2.TableDiff(context.Background(), root)
	assert.NoError(t, err)

	if len(added) != 1 || added[0] != "tbl1" || len(modified)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed, err = root.TableDiff(context.Background(), root2)
	assert.NoError(t, err)

	if len(removed) != 1 || removed[0] != "tbl1" || len(modified)+len(added) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	rowData, _ := createTestRowData(t, ddb.ValueReadWriter(), sch)
	tbl1Updated, _ := createTestTable(ddb.ValueReadWriter(), sch, rowData)

	root3, err := root.PutTable(context.Background(), "tbl1", tbl1Updated)
	assert.NoError(t, err)

	added, modified, removed, err = root3.TableDiff(context.Background(), root2)
	assert.NoError(t, err)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	added, modified, removed, err = root2.TableDiff(context.Background(), root3)
	assert.NoError(t, err)

	if len(modified) != 1 || modified[0] != "tbl1" || len(added)+len(removed) != 0 {
		t.Error("Bad table diff after adding a single table")
	}

	root4, err := root3.PutTable(context.Background(), "tbl2", tbl1)
	assert.NoError(t, err)

	added, modified, removed, err = root2.TableDiff(context.Background(), root4)
	assert.NoError(t, err)
	if len(modified) != 1 || modified[0] != "tbl1" || len(removed) != 1 || removed[0] != "tbl2" || +len(added) != 0 {
		t.Error("Bad table diff after adding a second table")
	}

	added, modified, removed, err = root4.TableDiff(context.Background(), root2)
	assert.NoError(t, err)
	if len(modified) != 1 || modified[0] != "tbl1" || len(added) != 1 || added[0] != "tbl2" || +len(removed) != 0 {
		t.Error("Bad table diff after adding a second table")
	}
}
