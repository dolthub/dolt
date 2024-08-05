// Copyright 2019 Dolthub, Inc.
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
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
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

	AutoIncrement bool `noms:"auto_increment,omitempty" json:"auto_increment,omitempty"`

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

	var constraints []schema.ColConstraint
	seenNotNull := false
	for _, nc := range encConstraints {
		c := nc.decodeColConstraint()
		// Prevent duplicate NOT NULL constraints
		if c.GetConstraintType() == schema.NotNullConstraintType {
			if seenNotNull {
				continue
			}
			seenNotNull = true
		}
		constraints = append(constraints, c)
	}

	return constraints
}

func encodeColumn(col schema.Column) encodedColumn {
	return encodedColumn{
		Tag:           col.Tag,
		Name:          col.Name,
		Kind:          col.KindString(),
		IsPartOfPK:    col.IsPartOfPK,
		TypeInfo:      encodeTypeInfo(col.TypeInfo),
		Default:       col.Default,
		AutoIncrement: col.AutoIncrement,
		Comment:       col.Comment,
		Constraints:   encodeAllColConstraints(col.Constraints),
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
	return schema.NewColumnWithTypeInfo(nfd.Name, nfd.Tag, typeInfo, nfd.IsPartOfPK, nfd.Default, nfd.AutoIncrement, nfd.Comment, colConstraints...)
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
	Name            string              `noms:"name" json:"name"`
	Tags            []uint64            `noms:"tags" json:"tags"`
	Comment         string              `noms:"comment" json:"comment"`
	Unique          bool                `noms:"unique" json:"unique"`
	Spatial         bool                `noms:"spatial,omitempty" json:"spatial,omitempty"`
	FullText        bool                `noms:"fulltext,omitempty" json:"fulltext,omitempty"`
	IsSystemDefined bool                `noms:"hidden,omitempty" json:"hidden,omitempty"` // Was previously named Hidden, do not change noms name
	PrefixLengths   []uint16            `noms:"prefixLengths,omitempty" json:"prefixLengths,omitempty"`
	FullTextInfo    encodedFullTextInfo `noms:"fulltext_info,omitempty" json:"fulltext_info,omitempty"`
}

type encodedFullTextInfo struct {
	ConfigTable      string   `noms:"config_table" json:"config_table"`
	PositionTable    string   `noms:"position_table" json:"position_table"`
	DocCountTable    string   `noms:"doc_count_table" json:"doc_count_table"`
	GlobalCountTable string   `noms:"global_count_table" json:"global_count_table"`
	RowCountTable    string   `noms:"row_count_table" json:"row_count_table"`
	KeyType          uint8    `noms:"key_type" json:"key_type"`
	KeyName          string   `noms:"key_name" json:"key_name"`
	KeyPositions     []uint16 `noms:"key_positions" json:"key_positions"`
}

type encodedCheck struct {
	Name       string `noms:"name" json:"name"`
	Expression string `noms:"expression" json:"expression"`
	Enforced   bool   `noms:"enforced" json:"enforced"`
}

type encodedSchemaData struct {
	Columns          []encodedColumn  `noms:"columns" json:"columns"`
	IndexCollection  []encodedIndex   `noms:"idxColl,omitempty" json:"idxColl,omitempty"`
	CheckConstraints []encodedCheck   `noms:"checks,omitempty" json:"checks,omitempty"`
	PkOrdinals       []int            `noms:"pkOrdinals,omitempty" json:"pkOrdinals,omitempty"`
	Collation        schema.Collation `noms:"collation,omitempty" json:"collation,omitempty"`
}

func (sd *encodedSchemaData) Copy() *encodedSchemaData {
	var columns []encodedColumn
	if sd.Columns != nil {
		columns = make([]encodedColumn, len(sd.Columns))
		for i, column := range sd.Columns {
			columns[i] = column
		}
	}

	var idxCol []encodedIndex
	if sd.IndexCollection != nil {
		idxCol = make([]encodedIndex, len(sd.IndexCollection))
		for i, idx := range sd.IndexCollection {
			idxCol[i] = idx
			idxCol[i].Tags = make([]uint64, len(idx.Tags))
			for j, tag := range idx.Tags {
				idxCol[i].Tags[j] = tag
			}
			if len(idx.PrefixLengths) > 0 {
				idxCol[i].PrefixLengths = make([]uint16, len(idx.PrefixLengths))
				for j, prefixLength := range idx.PrefixLengths {
					idxCol[i].PrefixLengths[j] = prefixLength
				}
			}
		}
	}

	var checks []encodedCheck
	if sd.CheckConstraints != nil {
		checks = make([]encodedCheck, len(sd.CheckConstraints))
		for i, check := range sd.CheckConstraints {
			checks[i] = check
		}
	}

	var pkOrdinals []int
	if sd.PkOrdinals != nil {
		pkOrdinals = make([]int, len(sd.PkOrdinals))
		for i, j := range sd.PkOrdinals {
			pkOrdinals[i] = j
		}
	}

	return &encodedSchemaData{
		Columns:          columns,
		IndexCollection:  idxCol,
		CheckConstraints: checks,
		PkOrdinals:       pkOrdinals,
		Collation:        sd.Collation,
	}
}

func toSchemaData(sch schema.Schema) (encodedSchemaData, error) {
	allCols := sch.GetAllCols()
	encCols := make([]encodedColumn, allCols.Size())

	i := 0
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		encCols[i] = encodeColumn(col)
		i++

		return false, nil
	})

	if err != nil {
		return encodedSchemaData{}, err
	}

	encodedIndexes := make([]encodedIndex, sch.Indexes().Count())
	for i, index := range sch.Indexes().AllIndexes() {
		props := index.FullTextProperties()
		encodedIndexes[i] = encodedIndex{
			Name:            index.Name(),
			Tags:            index.IndexedColumnTags(),
			Comment:         index.Comment(),
			Unique:          index.IsUnique(),
			Spatial:         index.IsSpatial(),
			FullText:        index.IsFullText(),
			IsSystemDefined: !index.IsUserDefined(),
			PrefixLengths:   index.PrefixLengths(),
			FullTextInfo: encodedFullTextInfo{
				ConfigTable:      props.ConfigTable,
				PositionTable:    props.PositionTable,
				DocCountTable:    props.DocCountTable,
				GlobalCountTable: props.GlobalCountTable,
				RowCountTable:    props.RowCountTable,
				KeyType:          props.KeyType,
				KeyName:          props.KeyName,
				KeyPositions:     props.KeyPositions,
			},
		}
	}

	encodedChecks := make([]encodedCheck, sch.Checks().Count())
	checks := sch.Checks()
	for i, check := range checks.AllChecks() {
		encodedChecks[i] = encodedCheck{
			Name:       check.Name(),
			Expression: check.Expression(),
			Enforced:   check.Enforced(),
		}
	}

	return encodedSchemaData{
		Columns:          encCols,
		IndexCollection:  encodedIndexes,
		CheckConstraints: encodedChecks,
		PkOrdinals:       sch.GetPkOrdinals(),
		Collation:        sch.GetCollation(),
	}, nil
}

func (sd encodedSchemaData) decodeSchema() (schema.Schema, error) {
	numCols := len(sd.Columns)
	cols := make([]schema.Column, numCols)

	var err error
	for i, col := range sd.Columns {
		cols[i], err = col.decodeColumn()
		if err != nil {
			return nil, err
		}
	}

	colColl := schema.NewColCollection(cols...)

	sch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}
	sch.SetCollation(sd.Collation)

	if sd.PkOrdinals != nil {
		err := sch.SetPkOrdinals(sd.PkOrdinals)
		if err != nil {
			return nil, err
		}
	}

	for _, encodedIndex := range sd.IndexCollection {
		_, err := sch.Indexes().UnsafeAddIndexByColTags(
			encodedIndex.Name,
			encodedIndex.Tags,
			encodedIndex.PrefixLengths,
			schema.IndexProperties{
				IsUnique:      encodedIndex.Unique,
				IsSpatial:     encodedIndex.Spatial,
				IsFullText:    encodedIndex.FullText,
				IsUserDefined: !encodedIndex.IsSystemDefined,
				Comment:       encodedIndex.Comment,
				FullTextProperties: schema.FullTextProperties{
					ConfigTable:      encodedIndex.FullTextInfo.ConfigTable,
					PositionTable:    encodedIndex.FullTextInfo.PositionTable,
					DocCountTable:    encodedIndex.FullTextInfo.DocCountTable,
					GlobalCountTable: encodedIndex.FullTextInfo.GlobalCountTable,
					RowCountTable:    encodedIndex.FullTextInfo.RowCountTable,
					KeyType:          encodedIndex.FullTextInfo.KeyType,
					KeyName:          encodedIndex.FullTextInfo.KeyName,
					KeyPositions:     encodedIndex.FullTextInfo.KeyPositions,
				},
			},
		)
		if err != nil {
			return nil, err
		}
	}

	for _, encodedCheck := range sd.CheckConstraints {
		_, err := sch.Checks().AddCheck(
			encodedCheck.Name,
			encodedCheck.Expression,
			encodedCheck.Enforced,
		)
		if err != nil {
			return nil, err
		}
	}

	return sch, nil
}

// MarshalSchema takes a Schema and converts it to a types.Value
func MarshalSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
	// Anyone calling this is going to serialize this to disk, so it's our last line of defense against defective schemas.
	// Business logic should catch errors before this point, but this is a failsafe.
	err := schema.ValidateColumnConstraints(sch.GetAllCols())
	if err != nil {
		return nil, err
	}

	err = schema.ValidateForInsert(sch.GetAllCols())
	if err != nil {
		return nil, err
	}

	if vrw.Format().VersionString() != types.Format_DOLT.VersionString() {
		for _, idx := range sch.Indexes().AllIndexes() {
			if idx.IsSpatial() {
				return nil, fmt.Errorf("spatial indexes are only supported in storage format __DOLT__")
			}
		}
	}

	if vrw.Format().UsesFlatbuffers() {
		return SerializeSchema(ctx, vrw, sch)
	}

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

type schCacheData struct {
	schema schema.Schema
}

var schemaCacheMu *sync.Mutex = &sync.Mutex{}
var unmarshalledSchemaCache = map[hash.Hash]schCacheData{}

// UnmarshalSchema takes a types.Value representing a Schema and Unmarshalls it into a schema.Schema.
func UnmarshalSchema(ctx context.Context, nbf *types.NomsBinFormat, schemaVal types.Value) (schema.Schema, error) {
	var sch schema.Schema
	var err error
	if nbf.UsesFlatbuffers() {
		sch, err = DeserializeSchema(ctx, nbf, schemaVal)
		if err != nil {
			return nil, err
		}
	} else {
		var sd encodedSchemaData
		err = marshal.Unmarshal(ctx, nbf, schemaVal, &sd)
		if err != nil {
			return nil, err
		}

		sch, err = sd.decodeSchema()
		if err != nil {
			return nil, err
		}
	}

	if nbf.VersionString() != types.Format_DOLT.VersionString() {
		for _, idx := range sch.Indexes().AllIndexes() {
			if idx.IsSpatial() {
				return nil, fmt.Errorf("spatial indexes are only supported in storage format __DOLT__")
			}
		}
	}

	return sch, nil
}

// UnmarshalSchemaAtAddr returns the schema at the given address, using the schema cache if possible.
func UnmarshalSchemaAtAddr(ctx context.Context, vr types.ValueReader, addr hash.Hash) (schema.Schema, error) {
	schemaCacheMu.Lock()
	cachedData, ok := unmarshalledSchemaCache[addr]
	schemaCacheMu.Unlock()

	if ok {
		cachedSch := cachedData.schema
		return cachedSch.Copy(), nil
	}

	schemaVal, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	sch, err := UnmarshalSchema(ctx, vr.Format(), schemaVal)
	if err != nil {
		return nil, err
	}

	d := schCacheData{
		schema: sch,
	}

	schemaCacheMu.Lock()
	unmarshalledSchemaCache[addr] = d
	schemaCacheMu.Unlock()

	return sch, nil
}
