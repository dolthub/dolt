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
	Name            string   `noms:"name" json:"name"`
	Tags            []uint64 `noms:"tags" json:"tags"`
	Comment         string   `noms:"comment" json:"comment"`
	Unique          bool     `noms:"unique" json:"unique"`
	Spatial         bool     `noms:"spatial,omitempty" json:"spatial,omitempty"`
	IsSystemDefined bool     `noms:"hidden,omitempty" json:"hidden,omitempty"` // Was previously named Hidden, do not change noms name
	PrefixLengths   []uint16 `noms:"prefixLengths,omitempty" json:"prefixLengths,omitempty"`
}

type encodedCheck struct {
	Name       string `noms:"name" json:"name"`
	Expression string `noms:"expression" json:"expression"`
	Enforced   bool   `noms:"enforced" json:"enforced"`
}

type schemaData struct {
	Columns          []encodedColumn  `noms:"columns" json:"columns"`
	IndexCollection  []encodedIndex   `noms:"idxColl,omitempty" json:"idxColl,omitempty"`
	CheckConstraints []encodedCheck   `noms:"checks,omitempty" json:"checks,omitempty"`
	PkOrdinals       []int            `noms:"pkOrdinals,omitempty" json:"pkOrdinals,omitEmpty"`
	Collation        schema.Collation `noms:"collation,omitempty" json:"collation,omitempty"`
}

func (sd *schemaData) Copy() *schemaData {
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

	return &schemaData{
		Columns:          columns,
		IndexCollection:  idxCol,
		CheckConstraints: checks,
		PkOrdinals:       pkOrdinals,
		Collation:        sd.Collation,
	}
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
			Name:            index.Name(),
			Tags:            index.IndexedColumnTags(),
			Comment:         index.Comment(),
			Unique:          index.IsUnique(),
			Spatial:         index.IsSpatial(),
			IsSystemDefined: !index.IsUserDefined(),
			PrefixLengths:   index.PrefixLengths(),
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

	return schemaData{
		Columns:          encCols,
		IndexCollection:  encodedIndexes,
		CheckConstraints: encodedChecks,
		PkOrdinals:       sch.GetPkOrdinals(),
		Collation:        sch.GetCollation(),
	}, nil
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

	colColl := schema.NewColCollection(cols...)

	sch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}
	sch.SetCollation(sd.Collation)

	return sch, nil
}

func (sd schemaData) addChecksIndexesAndPkOrderingToSchema(sch schema.Schema) error {
	// initialize pk order before adding indexes
	if sd.PkOrdinals != nil {
		err := sch.SetPkOrdinals(sd.PkOrdinals)
		if err != nil {
			return err
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
				IsUserDefined: !encodedIndex.IsSystemDefined,
				Comment:       encodedIndex.Comment,
			},
		)
		if err != nil {
			return err
		}
	}

	for _, encodedCheck := range sd.CheckConstraints {
		_, err := sch.Checks().AddCheck(
			encodedCheck.Name,
			encodedCheck.Expression,
			encodedCheck.Enforced,
		)
		if err != nil {
			return err
		}
	}

	sch.SetCollation(sd.Collation)

	return nil
}

// MarshalSchemaAsNomsValue takes a Schema and converts it to a types.Value
func MarshalSchemaAsNomsValue(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
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
	all   *schema.ColCollection
	pk    *schema.ColCollection
	nonPK *schema.ColCollection
	sd    *schemaData
}

var schemaCacheMu *sync.Mutex = &sync.Mutex{}
var unmarshalledSchemaCache = map[hash.Hash]schCacheData{}

// UnmarshalSchemaNomsValue takes a types.Value instance and Unmarshalls it into a Schema.
func UnmarshalSchemaNomsValue(ctx context.Context, nbf *types.NomsBinFormat, schemaVal types.Value) (schema.Schema, error) {
	h, err := schemaVal.Hash(nbf)
	if err != nil {
		return nil, err
	}

	schemaCacheMu.Lock()
	cachedData, ok := unmarshalledSchemaCache[h]
	schemaCacheMu.Unlock()

	if ok {
		cachedSch := schema.SchemaFromColCollections(cachedData.all, cachedData.pk, cachedData.nonPK)
		sd := cachedData.sd.Copy()
		err := sd.addChecksIndexesAndPkOrderingToSchema(cachedSch)
		if err != nil {
			return nil, err
		}

		return cachedSch, nil
	}

	var sd schemaData
	if nbf.UsesFlatbuffers() {
		sch, err := DeserializeSchema(ctx, nbf, schemaVal)
		if err != nil {
			return nil, err
		}
		sd, err = toSchemaData(sch)
	} else {
		err = marshal.Unmarshal(ctx, nbf, schemaVal, &sd)
	}
	if err != nil {
		return nil, err
	}

	sch, err := sd.decodeSchema()
	if err != nil {
		return nil, err
	}

	if nbf.VersionString() != types.Format_DOLT.VersionString() {
		for _, idx := range sch.Indexes().AllIndexes() {
			if idx.IsSpatial() {
				return nil, fmt.Errorf("spatial indexes are only supported in storage format __DOLT__")
			}
		}
	}

	if sd.PkOrdinals == nil {
		// schemaData will not have PK ordinals in old versions of Dolt
		// this sets the default PK ordinates for subsequent cache lookups
		sd.PkOrdinals = sch.GetPkOrdinals()
	}

	err = sd.addChecksIndexesAndPkOrderingToSchema(sch)
	if err != nil {
		return nil, err
	}

	d := schCacheData{
		all:   sch.GetAllCols(),
		pk:    sch.GetPKCols(),
		nonPK: sch.GetNonPKCols(),
		sd:    sd.Copy(),
	}

	schemaCacheMu.Lock()
	unmarshalledSchemaCache[h] = d
	schemaCacheMu.Unlock()

	return sch, nil
}
