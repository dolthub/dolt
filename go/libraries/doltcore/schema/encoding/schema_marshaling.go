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
	"errors"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Correct Marshalling & Unmarshalling is essential to compatibility across Dolt versions
// any changes to the fields of Schema or other persisted objects must be append only, no
// fields can ever be removed without breaking compatibility.
//
// the marshalling annotations of new fields must have the "omitempty" option to allow newer
// versions of Dolt to read objects serialized by older Dolt versions where the field did not
// yet exists. However, all fields must always be written.
type encodedColumn struct {
	Tag uint64 `noms:"tag" json:"tag"`

	// Name is the name of the field
	Name string `noms:"name" json:"name"`

	// Kind is the type of the field.  See types/noms_kind.go in the liquidata fork for valid values
	Kind string `noms:"kind" json:"kind"`

	IsPartOfPK bool `noms:"is_part_of_pk" json:"is_part_of_pk"`

	TypeInfo encodedTypeInfo `noms:"typeinfo,omitempty" json:"typeinfo,omitempty"`

	Default string `noms:"default,omitempty" json:"default,omitempty"`

	Comment string `noms:"comment,omitempty" json:"comment,omitempty"`

	Constraints []encodedConstraint `noms:"col_constraints" json:"col_constraints"`

	// NB: all new fields must have the 'omitempty' annotation. See comment above
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
		col.Default,
		col.Comment,
		encodeAllColConstraints(col.Constraints),
	}
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
	return schema.NewColumnWithTypeInfo(nfd.Name, nfd.Tag, typeInfo, nfd.IsPartOfPK, nfd.Default, nfd.Comment, colConstraints...)
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

type encodedIndex struct {
	Name    string   `noms:"name" json:"name"`
	Tags    []uint64 `noms:"tags" json:"tags"`
	Comment string   `noms:"comment" json:"comment"`
	Unique  bool     `noms:"unique" json:"unique"`
	Hidden  bool     `noms:"hidden,omitempty" json:"hidden,omitempty"`
}

type schemaData struct {
	Columns         []encodedColumn `noms:"columns" json:"columns"`
	IndexCollection []encodedIndex  `noms:"idxColl,omitempty" json:"idxColl,omitempty"`
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

	encodedIndexes := make([]encodedIndex, sch.Indexes().Count())
	for i, index := range sch.Indexes().AllIndexes() {
		encodedIndexes[i] = encodedIndex{
			Name:    index.Name(),
			Tags:    index.IndexedColumnTags(),
			Comment: index.Comment(),
			Unique:  index.IsUnique(),
		}
	}

	return schemaData{encCols, encodedIndexes}, nil
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

	sch := schema.SchemaFromCols(colColl)

	for _, encodedIndex := range sd.IndexCollection {
		_, err = sch.Indexes().UnsafeAddIndexByColTags(encodedIndex.Name, encodedIndex.Tags, schema.IndexProperties{IsUnique: encodedIndex.Unique, Comment: encodedIndex.Comment})
		if err != nil {
			return nil, err
		}
	}

	return sch, nil
}

// MarshalSchemaAsNomsValue takes a Schema and converts it to a types.Value
func MarshalSchemaAsNomsValue(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
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

// UnmarshalSchemaNomsValue takes a types.Value instance and Unmarshalls it into a Schema.
func UnmarshalSchemaNomsValue(ctx context.Context, nbf *types.NomsBinFormat, schemaVal types.Value) (schema.Schema, error) {
	var sd schemaData
	err := marshal.Unmarshal(ctx, nbf, schemaVal, &sd)

	if err != nil {
		return nil, err
	}

	return sd.decodeSchema()
}

type superSchemaData struct {
	Columns  []encodedColumn     `noms:"columns" json:"columns"`
	TagNames map[uint64][]string `noms:"col_constraints" json:"col_constraints"`
}

func toSuperSchemaData(ss *schema.SuperSchema) (superSchemaData, error) {
	encCols := make([]encodedColumn, ss.Size())
	tn := make(map[uint64][]string)

	i := 0
	err := ss.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		encCols[i] = encodeColumn(col)
		tn[tag] = ss.AllColumnNames(tag)
		i++

		return false, nil
	})

	if err != nil {
		return superSchemaData{}, err
	}

	return superSchemaData{encCols, tn}, nil
}

func (ssd superSchemaData) decodeSuperSchema() (*schema.SuperSchema, error) {
	numCols := len(ssd.Columns)
	cols := make([]schema.Column, numCols)

	for i, col := range ssd.Columns {
		c, err := col.decodeColumn()
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		return nil, err
	}

	if ssd.TagNames == nil {
		ssd.TagNames = make(map[uint64][]string)
	}

	return schema.UnmarshalSuperSchema(colColl, ssd.TagNames), nil
}

// MarshalSuperSchemaAsNomsValue creates a Noms value from a SuperSchema to be written to a RootValue.
func MarshalSuperSchemaAsNomsValue(ctx context.Context, vrw types.ValueReadWriter, ss *schema.SuperSchema) (types.Value, error) {
	ssd, err := toSuperSchemaData(ss)

	if err != nil {
		return types.EmptyStruct(vrw.Format()), err
	}

	val, err := marshal.Marshal(ctx, vrw, ssd)

	if err != nil {
		return types.EmptyStruct(vrw.Format()), err
	}

	if _, ok := val.(types.Struct); ok {
		return val, nil
	}

	return types.EmptyStruct(vrw.Format()), errors.New("Table Super Schema could not be converted to types.Struct")
}

// UnmarshalSuperSchemaNomsValue takes a Noms value read from a RootValue and constructs a SuperSchema from it.
func UnmarshalSuperSchemaNomsValue(ctx context.Context, nbf *types.NomsBinFormat, ssVal types.Value) (*schema.SuperSchema, error) {
	var ssd superSchemaData
	err := marshal.Unmarshal(ctx, nbf, ssVal, &ssd)

	if err != nil {
		return nil, err
	}

	return ssd.decodeSuperSchema()
}
