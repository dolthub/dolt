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

package dtables

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/temp_branch_control"
)

type BranchControl struct{}

var _ sql.Table = (*BranchControl)(nil)
var _ sql.InsertableTable = (*BranchControl)(nil)
var _ sql.ReplaceableTable = (*BranchControl)(nil)
var _ sql.UpdatableTable = (*BranchControl)(nil)
var _ sql.DeletableTable = (*BranchControl)(nil)
var _ sql.RowInserter = (*BranchControl)(nil)
var _ sql.RowReplacer = (*BranchControl)(nil)
var _ sql.RowUpdater = (*BranchControl)(nil)
var _ sql.RowDeleter = (*BranchControl)(nil)

// Name implements the interface sql.Table.
func (b *BranchControl) Name() string {
	return "dolt_branch_control"
}

// String implements the interface sql.Table.
func (b *BranchControl) String() string {
	return "dolt_branch_control"
}

// Schema implements the interface sql.Table.
func (b *BranchControl) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{
			Name:       "branch",
			Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
			Source:     "dolt_branch_control",
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "user",
			Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
			Source:     "dolt_branch_control",
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "host",
			Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
			Source:     "dolt_branch_control",
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "permissions",
			Type:       sql.MustCreateSetType([]string{"admin", "write", "destroy", "merge", "branch"}, sql.Collation_utf8mb4_0900_ai_ci),
			Source:     "dolt_branch_control",
			PrimaryKey: false,
		},
	}
}

// Collation implements the interface sql.Table.
func (b *BranchControl) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (b *BranchControl) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (b *BranchControl) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	for _, node := range temp_branch_control.StaticAccess.Nodes {
		rows = append(rows, sql.Row{
			node.Branch.String(),
			node.User.String(),
			node.Host.String(),
			uint64(node.Permissions),
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Inserter implements the interface sql.InsertableTable.
func (b *BranchControl) Inserter(context *sql.Context) sql.RowInserter {
	return b
}

// Replacer implements the interface sql.ReplaceableTable.
func (b *BranchControl) Replacer(ctx *sql.Context) sql.RowReplacer {
	return b
}

// Updater implements the interface sql.UpdatableTable.
func (b *BranchControl) Updater(ctx *sql.Context) sql.RowUpdater {
	return b
}

// Deleter implements the interface sql.DeletableTable.
func (b *BranchControl) Deleter(context *sql.Context) sql.RowDeleter {
	return b
}

// StatementBegin implements the interface sql.TableEditor.
func (b *BranchControl) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor.
func (b *BranchControl) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (b *BranchControl) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Insert implements the interface sql.RowInserter.
func (b *BranchControl) Insert(ctx *sql.Context, row sql.Row) error {
	temp_branch_control.StaticAccess.RWMutex.Lock()
	defer temp_branch_control.StaticAccess.RWMutex.Unlock()

	branchLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, row[0].(string), '\\')
	if err != nil {
		return err
	}
	userLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, row[1].(string), '\\')
	if err != nil {
		return err
	}
	hostLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_ai_ci, row[2].(string), '\\')
	if err != nil {
		return err
	}
	perms := temp_branch_control.Permissions(row[3].(uint64))
	temp_branch_control.StaticAccess.Nodes = append(temp_branch_control.StaticAccess.Nodes, temp_branch_control.AccessNode{
		Branch:      branchLm,
		User:        userLm,
		Host:        hostLm,
		Permissions: perms,
	})
	return nil
}

// Update implements the interface sql.RowUpdater.
func (b *BranchControl) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	temp_branch_control.StaticAccess.RWMutex.Lock()
	defer temp_branch_control.StaticAccess.RWMutex.Unlock()

	oldBranch := old[0].(string)
	oldUser := old[1].(string)
	oldHost := old[2].(string)
	for i, node := range temp_branch_control.StaticAccess.Nodes {
		if node.Branch.String() == oldBranch && node.User.String() == oldUser && node.Host.String() == oldHost {
			branchLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, new[0].(string), '\\')
			if err != nil {
				return err
			}
			userLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, new[1].(string), '\\')
			if err != nil {
				return err
			}
			hostLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_ai_ci, new[2].(string), '\\')
			if err != nil {
				return err
			}
			perms := temp_branch_control.Permissions(new[3].(uint64))
			temp_branch_control.StaticAccess.Nodes[i] = temp_branch_control.AccessNode{
				Branch:      branchLm,
				User:        userLm,
				Host:        hostLm,
				Permissions: perms,
			}
			return nil
		}
	}
	return fmt.Errorf("could not find row with pk [`%s`, `%s`, `%s`]", oldBranch, oldUser, oldHost)
}

// Delete implements the interface sql.RowDeleter.
func (b *BranchControl) Delete(ctx *sql.Context, row sql.Row) error {
	temp_branch_control.StaticAccess.RWMutex.Lock()
	defer temp_branch_control.StaticAccess.RWMutex.Unlock()

	staticAccess := temp_branch_control.StaticAccess
	branch := row[0].(string)
	user := row[1].(string)
	host := row[2].(string)
	for i, node := range staticAccess.Nodes {
		if node.Branch.String() == branch && node.User.String() == user && node.Host.String() == host {
			lastNode := len(staticAccess.Nodes) - 1
			staticAccess.Nodes[i], staticAccess.Nodes[lastNode] = staticAccess.Nodes[lastNode], staticAccess.Nodes[i]
			staticAccess.Nodes = staticAccess.Nodes[:lastNode]
			return nil
		}
	}
	return fmt.Errorf("could not find row with pk [`%s`, `%s`, `%s`]", branch, user, host)
}

// Close implements the interface sql.Closer.
func (b *BranchControl) Close(context *sql.Context) error {
	return nil
}

type BranchControlProtected struct{}

var _ sql.Table = (*BranchControlProtected)(nil)
var _ sql.InsertableTable = (*BranchControlProtected)(nil)
var _ sql.ReplaceableTable = (*BranchControlProtected)(nil)
var _ sql.UpdatableTable = (*BranchControlProtected)(nil)
var _ sql.DeletableTable = (*BranchControlProtected)(nil)
var _ sql.RowInserter = (*BranchControlProtected)(nil)
var _ sql.RowReplacer = (*BranchControlProtected)(nil)
var _ sql.RowUpdater = (*BranchControlProtected)(nil)
var _ sql.RowDeleter = (*BranchControlProtected)(nil)

// Name implements the interface sql.Table.
func (b *BranchControlProtected) Name() string {
	return "dolt_branch_control_protected"
}

// String implements the interface sql.Table.
func (b *BranchControlProtected) String() string {
	return "dolt_branch_control_protected"
}

// Schema implements the interface sql.Table.
func (b *BranchControlProtected) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{
			Name:       "branch",
			Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
			Source:     "dolt_branch_control_protected",
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "user",
			Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
			Source:     "dolt_branch_control_protected",
			PrimaryKey: true,
		},
		&sql.Column{
			Name:       "host",
			Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
			Source:     "dolt_branch_control_protected",
			PrimaryKey: true,
		},
	}
}

// Collation implements the interface sql.Table.
func (b *BranchControlProtected) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (b *BranchControlProtected) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (b *BranchControlProtected) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	for _, node := range temp_branch_control.StaticCreation.Nodes {
		rows = append(rows, sql.Row{
			node.Branch.String(),
			node.User.String(),
			node.Host.String(),
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Inserter implements the interface sql.InsertableTable.
func (b *BranchControlProtected) Inserter(context *sql.Context) sql.RowInserter {
	return b
}

// Replacer implements the interface sql.ReplaceableTable.
func (b *BranchControlProtected) Replacer(ctx *sql.Context) sql.RowReplacer {
	return b
}

// Updater implements the interface sql.UpdatableTable.
func (b *BranchControlProtected) Updater(ctx *sql.Context) sql.RowUpdater {
	return b
}

// Deleter implements the interface sql.DeletableTable.
func (b *BranchControlProtected) Deleter(context *sql.Context) sql.RowDeleter {
	return b
}

// StatementBegin implements the interface sql.TableEditor.
func (b *BranchControlProtected) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor.
func (b *BranchControlProtected) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (b *BranchControlProtected) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Insert implements the interface sql.RowInserter.
func (b *BranchControlProtected) Insert(ctx *sql.Context, row sql.Row) error {
	temp_branch_control.StaticCreation.RWMutex.Lock()
	defer temp_branch_control.StaticCreation.RWMutex.Unlock()

	branchLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, row[0].(string), '\\')
	if err != nil {
		return err
	}
	userLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, row[1].(string), '\\')
	if err != nil {
		return err
	}
	hostLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_ai_ci, row[2].(string), '\\')
	if err != nil {
		return err
	}
	temp_branch_control.StaticCreation.Nodes = append(temp_branch_control.StaticCreation.Nodes, temp_branch_control.CreationNode{
		Branch: branchLm,
		User:   userLm,
		Host:   hostLm,
	})
	return nil
}

// Update implements the interface sql.RowUpdater.
func (b *BranchControlProtected) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	temp_branch_control.StaticCreation.RWMutex.Lock()
	defer temp_branch_control.StaticCreation.RWMutex.Unlock()

	oldBranch := old[0].(string)
	oldUser := old[1].(string)
	oldHost := old[2].(string)
	for i, node := range temp_branch_control.StaticCreation.Nodes {
		if node.Branch.String() == oldBranch && node.User.String() == oldUser && node.Host.String() == oldHost {
			branchLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, new[0].(string), '\\')
			if err != nil {
				return err
			}
			userLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_bin, new[1].(string), '\\')
			if err != nil {
				return err
			}
			hostLm, err := expression.ConstructLikeMatcher(sql.Collation_utf8mb4_0900_ai_ci, new[2].(string), '\\')
			if err != nil {
				return err
			}
			temp_branch_control.StaticCreation.Nodes[i] = temp_branch_control.CreationNode{
				Branch: branchLm,
				User:   userLm,
				Host:   hostLm,
			}
			return nil
		}
	}
	return fmt.Errorf("could not find row with pk [`%s`, `%s`, `%s`]", oldBranch, oldUser, oldHost)
}

// Delete implements the interface sql.RowDeleter.
func (b *BranchControlProtected) Delete(ctx *sql.Context, row sql.Row) error {
	temp_branch_control.StaticCreation.RWMutex.Lock()
	defer temp_branch_control.StaticCreation.RWMutex.Unlock()

	staticCreation := temp_branch_control.StaticCreation
	branch := row[0].(string)
	user := row[1].(string)
	host := row[2].(string)
	for i, node := range staticCreation.Nodes {
		if node.Branch.String() == branch && node.User.String() == user && node.Host.String() == host {
			lastNode := len(staticCreation.Nodes) - 1
			staticCreation.Nodes[i], staticCreation.Nodes[lastNode] = staticCreation.Nodes[lastNode], staticCreation.Nodes[i]
			staticCreation.Nodes = staticCreation.Nodes[:lastNode]
			return nil
		}
	}
	return fmt.Errorf("could not find row with pk [`%s`, `%s`, `%s`]", branch, user, host)
}

// Close implements the interface sql.Closer.
func (b *BranchControlProtected) Close(context *sql.Context) error {
	return nil
}
