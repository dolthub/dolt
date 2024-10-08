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

package doltdb

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	stypes "github.com/dolthub/dolt/go/store/types"
)

type doltCIWorkflowsTableCreator struct{}

var _ DoltCITableCreator = (*doltCIWorkflowsTableCreator)(nil)

func NewDoltCIWorkflowsTableCreator() *doltCIWorkflowsTableCreator {
	return &doltCIWorkflowsTableCreator{}
}

func (d *doltCIWorkflowsTableCreator) CreateTable(ctx context.Context, rv RootValue) (RootValue, error) {
	found, err := rv.HasTable(ctx, TableName{Name: WorkflowsTableName})
	if err != nil {
		return nil, err
	}
	if found {
		return rv, nil
	}

	colCollection := schema.NewColCollection(
		schema.Column{
			Name:          WorkflowsNameColName,
			Tag:           schema.WorkflowsNameTag,
			Kind:          stypes.StringKind,
			IsPartOfPK:    true,
			TypeInfo:      typeinfo.FromKind(stypes.StringKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          WorkflowsCreatedAtColName,
			Tag:           schema.WorkflowsCreatedAtTag,
			Kind:          stypes.TimestampKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.TimestampKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
		schema.Column{
			Name:          WorkflowsUpdatedAtColName,
			Tag:           schema.WorkflowsUpdatedAtTag,
			Kind:          stypes.TimestampKind,
			IsPartOfPK:    false,
			TypeInfo:      typeinfo.FromKind(stypes.TimestampKind),
			Default:       "",
			AutoIncrement: false,
			Comment:       "",
			Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
		},
	)

	newSchema, err := schema.NewSchema(colCollection, nil, schema.Collation_Default, nil, nil)
	if err != nil {
		return nil, err
	}

	// underlying table doesn't exist. Record this, then create the table.
	return CreateEmptyTable(ctx, rv, TableName{Name: WorkflowsTableName}, newSchema)
}
