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

package encoding

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type encodedColumn struct {
	Tag uint64 `noms:"tag" json:"tag"`

	// Name is the name of the field
	Name string `noms:"name" json:"name"`

	// Kind is the type of the field.  See types/noms_kind.go in the liquidata fork for valid values
	Kind string `noms:"kind" json:"kind"`

	IsPartOfPK bool `noms:"is_part_of_pk" json:"is_part_of_pk"`

	TypeInfo encodedTypeInfo `noms:"typeinfo,omitempty" json:"typeinfo,omitempty"`

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
		c := nc.decodeColConstraint()
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
		encodeTypeInfo(col.TypeInfo),
		encodeAllColConstraints(col.Constraints)}
}

func (nfd encodedColumn) decodeColumn() (schema.Column, error) {
	var typeInfo typeinfo.TypeInfo
	var err error
	if nfd.TypeInfo.Type != "" {
		typeInfo, err = nfd.TypeInfo.decodeTypeInfo()
		if err != nil {
			return schema.Column{}, err
		}
	} else if nfd.Kind != "" {
		typeInfo = typeinfo.FromKind(schema.LwrStrToKind[nfd.Kind])
	} else {
		return schema.Column{}, errors.New("cannot decode column due to unknown schema format")
	}
	colConstraints := decodeAllColConstraint(nfd.Constraints)
	return schema.NewColumnWithTypeInfo(nfd.Name, nfd.Tag, typeInfo, nfd.IsPartOfPK, colConstraints...)
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

type encodedTypeInfo struct {
	Type   string            `noms:"type" json:"type"`
	Params map[string]string `noms:"params" json:"params"`
}

func encodeTypeInfo(ti typeinfo.TypeInfo) encodedTypeInfo {
	return encodedTypeInfo{ti.GetTypeIdentifier().String(), ti.GetTypeParams()}
}

func (enc encodedTypeInfo) decodeTypeInfo() (typeinfo.TypeInfo, error) {
	id := typeinfo.ParseIdentifier(enc.Type)
	return typeinfo.FromTypeParams(id, enc.Params)
}

type schemaData struct {
	Columns []encodedColumn `noms:"columns" json:"columns"`
}

func toSchemaData(sch schema.Schema) (schemaData, error) {
	allCols := sch.GetAllCols()
	encCols := make([]encodedColumn, allCols.Size())

	i := 0
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		encCols[i] = encodeColumn(col)
		i++

		return false, nil
	})

	if err != nil {
		return schemaData{}, err
	}

	return schemaData{encCols}, nil
}

func (sd schemaData) decodeSchema() (schema.Schema, error) {
	numCols := len(sd.Columns)
	cols := make([]schema.Column, numCols)

	var err error
	for i, col := range sd.Columns {
		cols[i], err = col.decodeColumn()
		if err != nil {
			return nil, err
		}
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(colColl), nil
}

// MarshalAsNomsValue takes a Schema and converts it to a types.Value
func MarshalAsNomsValue(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
	sd, err := toSchemaData(sch)

	if err != nil {
		return types.EmptyStruct(vrw.Format()), err
	}

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
func UnmarshalNomsValue(ctx context.Context, nbf *types.NomsBinFormat, schemaVal types.Value) (schema.Schema, error) {
	var sd schemaData
	err := marshal.Unmarshal(ctx, nbf, schemaVal, &sd)

	if err != nil {
		return nil, err
	}

	return sd.decodeSchema()
}

// MarshalAsJson takes a Schema and returns a string containing it's json encoding
func MarshalAsJson(sch schema.Schema) (string, error) {
	sd, err := toSchemaData(sch)

	if err != nil {
		return "", err
	}

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
