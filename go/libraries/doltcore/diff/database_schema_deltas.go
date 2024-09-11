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

package diff

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// DatabaseSchemaDelta represents a change in the set of database schemas between two roots
type DatabaseSchemaDelta struct {
	FromName string
	ToName   string
}

func (d DatabaseSchemaDelta) IsAdd() bool {
	return d.FromName == "" && d.ToName != ""
}

func (d DatabaseSchemaDelta) IsDrop() bool {
	return d.FromName != "" && d.ToName == ""
}

func (d DatabaseSchemaDelta) CurName() string {
	if d.ToName != "" {
		return d.ToName
	}
	return d.FromName
}

// GetDatabaseSchemaDeltas returns a list of DatabaseSchemaDelta objects representing the changes in database schemas
func GetDatabaseSchemaDeltas(ctx context.Context, fromRoot, toRoot doltdb.RootValue) ([]DatabaseSchemaDelta, error) {
	fromNames, err := getDatabaseSchemaNames(ctx, fromRoot)
	if err != nil {
		return nil, err
	}

	toNames, err := getDatabaseSchemaNames(ctx, toRoot)
	if err != nil {
		return nil, err
	}

	// short circuit for common case where there are no schemas (dolt)
	if fromNames.Size() == 0 && toNames.Size() == 0 {
		return nil, nil
	}

	// generate a diff for each schema name that's present in one root but not the other
	var deltas []DatabaseSchemaDelta
	fromNames.Iterate(func(name string) (cont bool) {
		if !toNames.Contains(name) {
			deltas = append(deltas, DatabaseSchemaDelta{FromName: name})
		}
		return true
	})

	toNames.Iterate(func(name string) (cont bool) {
		if !fromNames.Contains(name) {
			deltas = append(deltas, DatabaseSchemaDelta{ToName: name})
		}
		return true
	})

	return deltas, nil
}

// GetStagedUnstagedDatabaseSchemaDeltas represents staged and unstaged changes as DatabaseSchemaDelta slices.
func GetStagedUnstagedDatabaseSchemaDeltas(ctx context.Context, roots doltdb.Roots) (staged, unstaged []DatabaseSchemaDelta, err error) {
	staged, err = GetDatabaseSchemaDeltas(ctx, roots.Head, roots.Staged)
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetDatabaseSchemaDeltas(ctx, roots.Staged, roots.Working)
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
}

func getDatabaseSchemaNames(ctx context.Context, root doltdb.RootValue) (*set.StrSet, error) {
	dbSchemaNames := set.NewEmptyStrSet()
	dbSchemas, err := root.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}
	for _, dbSchema := range dbSchemas {
		dbSchemaNames.Add(dbSchema.Name)
	}
	return dbSchemaNames, nil
}
