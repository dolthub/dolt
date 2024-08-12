package doltdb

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	stypes "github.com/dolthub/dolt/go/store/types"
)

type CIConfigTablesCreator interface {
	CreateCIConfigTables(ctx context.Context, rv RootValue) (RootValue, error)
}

type noopCIConfigTableCreator struct{}

func (n *noopCIConfigTableCreator) CreateCIConfigTables(ctx context.Context, rv RootValue) (RootValue, error) {
	return rv, nil
}

var _ CIConfigTablesCreator = (*noopCIConfigTableCreator)(nil)

type doltCIConfigTablesCreator struct {
}

func newDoltCIConfigTablesCreator() *doltCIConfigTablesCreator {
	return &doltCIConfigTablesCreator{}
}

func (d *doltCIConfigTablesCreator) createTableDoltCiWorkflows(ctx context.Context, rv RootValue) (RootValue, error) {
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

	return CreateEmptyTable(ctx, rv, TableName{Name: WorkflowsTableName}, newSchema)
}

func (d *doltCIConfigTablesCreator) CreateCIConfigTables(ctx context.Context, rv RootValue) (RootValue, error) {
	ciWorkflowsRv, err := d.createTableDoltCiWorkflows(ctx, rv)
	if err != nil {
		return nil, err
	}
	return ciWorkflowsRv, nil
}

var _ CIConfigTablesCreator = (*doltCIConfigTablesCreator)(nil)
