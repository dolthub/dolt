package encoding

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/marshal"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type encodedColumn struct {
	Tag uint64 `noms:"tag" json:"tag"`

	// Name is the name of the field
	Name string `noms:"name" json:"name"`

	// Kind is the type of the field.  See types/noms_kind.go in the liquidata fork for valid values
	Kind string `noms:"kind" json:"kind"`

	IsPartOfPK bool `noms:"is_part_of_pk" json:"is_part_of_pk"`

	Constraints []encodedConstraint `noms:"col_constraints" json:"col_constraints"`
}

func encodeAllColConstraints(constraints []schema.ColConstraint) []encodedConstraint {
	nomsConstraints := make([]encodedConstraint, len(constraints))

	for i, c := range constraints {
		nomsConstraints[i] = encodeColConstraint(c)
	}

	return nomsConstraints
}

func decodeAllColConstraint(encConstraints []encodedConstraint) []schema.ColConstraint {
	if len(encConstraints) == 0 {
		return nil
	}

	constraints := make([]schema.ColConstraint, len(encConstraints))

	for i, nc := range encConstraints {
		c := schema.ColConstraintFromTypeAndParams(nc.Type, nc.Params)
		constraints[i] = c
	}

	return constraints
}

func encodeColumn(col schema.Column) encodedColumn {
	return encodedColumn{
		col.Tag,
		col.Name,
		col.KindString(),
		col.IsPartOfPK,
		encodeAllColConstraints(col.Constraints)}
}

func (nfd encodedColumn) decodeColumn() schema.Column {
	colConstraints := decodeAllColConstraint(nfd.Constraints)
	return schema.NewColumn(nfd.Name, nfd.Tag, schema.LwrStrToKind[nfd.Kind], nfd.IsPartOfPK, colConstraints...)
}

type encodedConstraint struct {
	Type   string            `noms:"constraint_type" json:"constraint_type"`
	Params map[string]string `noms:"params" json:"params"`
}

func encodeColConstraint(constraint schema.ColConstraint) encodedConstraint {
	return encodedConstraint{constraint.GetConstraintType(), constraint.GetConstraintParams()}
}

func (encCnst encodedConstraint) decodeColConstraint() schema.ColConstraint {
	return schema.ColConstraintFromTypeAndParams(encCnst.Type, encCnst.Params)
}

type schemaData struct {
	Columns []encodedColumn `noms:"columns" json:"columns"`
}

func toSchemaData(sch schema.Schema) schemaData {
	allCols := sch.GetAllCols()
	encCols := make([]encodedColumn, allCols.Size())

	i := 0
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		encCols[i] = encodeColumn(col)
		i++

		return false
	})

	return schemaData{encCols}
}

func (sd schemaData) decodeSchema() (schema.Schema, error) {
	numCols := len(sd.Columns)
	cols := make([]schema.Column, numCols)

	for i, col := range sd.Columns {
		cols[i] = col.decodeColumn()
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(colColl), nil
}

// MarshalAsNomsValue takes a Schema and converts it to a types.Value
func MarshalAsNomsValue(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
	sd := toSchemaData(sch)
	val, err := marshal.Marshal(ctx, vrw, sd)

	if err != nil {
		return types.EmptyStruct(vrw.Format()), err
	}

	if _, ok := val.(types.Struct); ok {
		return val, nil
	}

	return types.EmptyStruct(vrw.Format()), errors.New("Table Schema could not be converted to types.Struct")
}

// UnmarshalNomsValue takes a types.Value instance and Unmarshalls it into a Schema.
func UnmarshalNomsValue(ctx context.Context, format *types.Format, schemaVal types.Value) (schema.Schema, error) {
	var sd schemaData
	err := marshal.Unmarshal(ctx, format, schemaVal, &sd)

	if err != nil {
		return nil, err
	}

	return sd.decodeSchema()
}

// MarshalAsJson takes a Schema and returns a string containing it's json encoding
func MarshalAsJson(sch schema.Schema) (string, error) {
	sd := toSchemaData(sch)
	jsonStr, err := json.MarshalIndent(sd, "", "  ")

	if err != nil {
		return "", err
	}

	return string(jsonStr), nil
}

// UnmarshalJson takes a json string and Unmarshalls it into a Schema.
func UnmarshalJson(jsonStr string) (schema.Schema, error) {
	var sd schemaData
	err := json.Unmarshal([]byte(jsonStr), &sd)

	if err != nil {
		return nil, err
	}

	return sd.decodeSchema()
}
