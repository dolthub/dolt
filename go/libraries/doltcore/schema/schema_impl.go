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

package schema

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var FeatureFlagKeylessSchema = true

// EmptySchema is an instance of a schema with no columns.
var EmptySchema = &schemaImpl{
	pkCols:          EmptyColColl,
	nonPKCols:       EmptyColColl,
	allCols:         EmptyColColl,
	indexCollection: NewIndexCollection(nil, nil),
}

type schemaImpl struct {
	pkCols, nonPKCols, allCols *ColCollection
	indexCollection            IndexCollection
	checkCollection            CheckCollection
	pkOrdinals                 []int
	collation                  Collation
	contentHashedFields        []uint64
	comment                    string
}

var _ Schema = (*schemaImpl)(nil)

var ErrInvalidPkOrdinals = errors.New("incorrect number of primary key ordinals")
var ErrMultipleNotNullConstraints = errors.New("multiple not null constraints on same column")

// NewSchema creates a fully defined schema from its parameters.
// This function should be updated when new components are added to Schema.
// If |len(pkOrdinals)| == 0, then the default ordinals are kept. |indexes| and |checks| may be nil.
func NewSchema(allCols *ColCollection, pkOrdinals []int, collation Collation, indexes IndexCollection, checks CheckCollection) (Schema, error) {
	sch, err := SchemaFromCols(allCols)
	if err != nil {
		return nil, err
	}

	if len(pkOrdinals) != 0 {
		err = sch.SetPkOrdinals(pkOrdinals)
		if err != nil {
			return nil, err
		}
	}

	sch.SetCollation(collation)

	if indexes != nil {
		indexColImpl := indexes.(*indexCollectionImpl)

		sch.(*schemaImpl).indexCollection = indexColImpl

		// Index collection contains information about the total list of columns and their definitions.
		// Do a simple sanity check here to make sure those columns match |allCols|.
		// TODO: Add an equality check between |allCols| and the cols that |indexes| refer to.

		if len(indexColImpl.pks) != sch.GetPKCols().Size() {
			return nil, fmt.Errorf("IndexCollection referring to %d pks while Schema refers to %d pks", len(indexColImpl.pks), sch.GetPKCols().Size())
		}
		for i, tag := range sch.GetPKCols().Tags {
			if indexColImpl.pks[i] != tag {
				return nil, fmt.Errorf("IndexCollection pk tags does not match Schema's pk tags")
			}
		}
	}

	if checks != nil {
		sch.(*schemaImpl).checkCollection = checks
	}

	return sch, nil
}

// SchemaFromCols creates a Schema from a collection of columns
//
// Deprecated: Use NewSchema instead.
func SchemaFromCols(allCols *ColCollection) (Schema, error) {
	var pkCols []Column
	var nonPKCols []Column

	defaultPkOrds := make([]int, 0)
	for i, c := range allCols.cols {
		if c.IsPartOfPK {
			pkCols = append(pkCols, c)
			defaultPkOrds = append(defaultPkOrds, i)
		} else {
			nonPKCols = append(nonPKCols, c)
		}
	}

	if len(pkCols) == 0 && !FeatureFlagKeylessSchema {
		return nil, ErrNoPrimaryKeyColumns
	}

	pkColColl := NewColCollection(pkCols...)
	nonPKColColl := NewColCollection(nonPKCols...)

	sch := SchemaFromColCollections(allCols, pkColColl, nonPKColColl)
	err := sch.SetPkOrdinals(defaultPkOrds)
	if err != nil {
		return nil, err
	}
	sch.SetCollation(Collation_Default)
	return sch, nil
}

// SchemaFromColCollections creates a schema from the three collections.
//
// Deprecated: Use NewSchema instead.
func SchemaFromColCollections(allCols, pkColColl, nonPKColColl *ColCollection) Schema {
	return &schemaImpl{
		pkCols:          pkColColl,
		nonPKCols:       nonPKColColl,
		allCols:         allCols,
		indexCollection: NewIndexCollection(allCols, pkColColl),
		checkCollection: NewCheckCollection(),
		pkOrdinals:      []int{},
		collation:       Collation_Default,
	}
}

func MustSchemaFromCols(typedColColl *ColCollection) Schema {
	sch, err := SchemaFromCols(typedColColl)
	if err != nil {
		panic(err)
	}
	return sch
}

// ValidateColumnConstraints removes any duplicate NOT NULL column constraints from schemas.
func ValidateColumnConstraints(allCols *ColCollection) error {
	for _, col := range allCols.cols {
		seenNotNull := false
		for _, cc := range col.Constraints {
			if cc.GetConstraintType() == NotNullConstraintType {
				if seenNotNull {
					return ErrMultipleNotNullConstraints
				}
				seenNotNull = true
			}
		}
	}
	return nil
}

// ValidateForInsert returns an error if the given schema cannot be written to the dolt database.
func ValidateForInsert(allCols *ColCollection) error {
	var seenPkCol bool
	for _, c := range allCols.cols {
		if c.IsPartOfPK {
			seenPkCol = true
			break
		}
		c.TypeInfo.ToSqlType()
	}

	if !seenPkCol && !FeatureFlagKeylessSchema {
		return ErrNoPrimaryKeyColumns
	}

	colNames := make(map[string]bool)
	colTags := make(map[uint64]bool)

	err := allCols.Iter(func(tag uint64, col Column) (stop bool, err error) {
		if _, ok := colTags[tag]; ok {
			return true, ErrColTagCollision
		}
		colTags[tag] = true

		if _, ok := colNames[strings.ToLower(col.Name)]; ok {
			return true, ErrColNameCollision
		}
		colNames[col.Name] = true

		if col.AutoIncrement && !(isAutoIncrementKind(col.Kind) || isAutoIncrementType(col.TypeInfo.ToSqlType().Type())) {
			return true, ErrNonAutoIncType
		}

		return false, nil
	})

	return err
}

// MaxRowStorageSize returns the storage length for Dolt types.
func MaxRowStorageSize(sch sql.Schema) int64 {
	var numBytesPerRow int64 = 0
	for _, col := range sch {
		switch n := col.Type.(type) {
		case sql.NumberType:
			numBytesPerRow += 8
		case sql.StringType:
			if gmstypes.IsTextBlob(n) {
				numBytesPerRow += 20
			} else {
				numBytesPerRow += n.MaxByteLength()
			}
		case gmstypes.BitType:
			numBytesPerRow += 8
		case sql.DatetimeType:
			numBytesPerRow += 8
		case sql.DecimalType:
			numBytesPerRow += int64(n.MaximumScale())
		case sql.EnumType:
			numBytesPerRow += 2
		case gmstypes.JsonType:
			numBytesPerRow += 20
		case sql.NullType:
			numBytesPerRow += 1
		case gmstypes.TimeType:
			numBytesPerRow += 16
		case sql.YearType:
			numBytesPerRow += 8
		default:
			panic(fmt.Sprintf("unknown type in create table: %s", n.String()))
		}
	}
	return numBytesPerRow
}

// isAutoIncrementKind returns true is |k| is a numeric kind.
func isAutoIncrementKind(k types.NomsKind) bool {
	return k == types.IntKind || k == types.UintKind || k == types.FloatKind
}

// isAutoIncrementType returns true is |t| is a numeric type.
// This is an alternative way for the numeric type check.
func isAutoIncrementType(t query.Type) bool {
	switch t {
	case query.Type_INT8, query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_INT64,
		query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64,
		query.Type_FLOAT32, query.Type_FLOAT64, query.Type_DECIMAL:
		return true
	default:
		return false
	}
}

// UnkeyedSchemaFromCols creates a schema without any primary keys to be used for displaying to users, tests, etc. Such
// unkeyed schemas are not suitable to be inserted into storage.
func UnkeyedSchemaFromCols(allCols *ColCollection) Schema {
	var nonPKCols []Column

	for _, c := range allCols.cols {
		c.IsPartOfPK = false
		c.Constraints = nil
		nonPKCols = append(nonPKCols, c)
	}

	pkColColl := NewColCollection()
	nonPKColColl := NewColCollection(nonPKCols...)

	return &schemaImpl{
		pkCols:          pkColColl,
		nonPKCols:       nonPKColColl,
		allCols:         nonPKColColl,
		indexCollection: NewIndexCollection(nil, nil),
		checkCollection: NewCheckCollection(),
		collation:       Collation_Default,
	}
}

// SchemaFromPKAndNonPKCols creates a Schema from a collection of the key columns, and the non-key columns.
//
// Deprecated: Use NewSchema instead.
func SchemaFromPKAndNonPKCols(pkCols, nonPKCols *ColCollection) (Schema, error) {
	allCols := make([]Column, pkCols.Size()+nonPKCols.Size())

	i := 0
	for _, c := range pkCols.cols {
		if !c.IsPartOfPK {
			panic("bug: attempting to add a column to the pk that isn't part of the pk")
		}

		allCols[i] = c
		i++
	}

	for _, c := range nonPKCols.cols {
		if c.IsPartOfPK {
			panic("bug: attempting to add a column that is part of the pk to the non-pk columns")
		}

		allCols[i] = c
		i++
	}

	allColColl := NewColCollection(allCols...)
	return SchemaFromColCollections(allColColl, pkCols, nonPKCols), nil
}

func (si *schemaImpl) GetComment() string {
	return si.comment
}

func (si *schemaImpl) SetComment(comment string) {
	si.comment = comment
}

// GetAllCols gets the collection of all columns (pk and non-pk)
func (si *schemaImpl) GetAllCols() *ColCollection {
	return si.allCols
}

// GetNonPKCols gets the collection of columns which are not part of the primary key.
func (si *schemaImpl) GetNonPKCols() *ColCollection {
	return si.nonPKCols
}

// GetPKCols gets the collection of columns which make the primary key.
func (si *schemaImpl) GetPKCols() *ColCollection {
	return si.pkCols
}

func (si *schemaImpl) GetPkOrdinals() []int {
	return si.pkOrdinals
}

func (si *schemaImpl) SetPkOrdinals(o []int) error {
	if si.pkCols.Size() == 0 {
		return nil
	} else if o == nil || len(o) != si.pkCols.Size() {
		var found int
		if o == nil {
			found = 0
		} else {
			found = len(o)
		}
		return fmt.Errorf("%w: expected '%d', found '%d'", ErrInvalidPkOrdinals, si.pkCols.Size(), found)
	}

	si.pkOrdinals = o
	newPks := make([]Column, si.pkCols.Size())
	newPkTags := make([]uint64, si.pkCols.Size())
	for i, j := range si.pkOrdinals {
		pkCol := si.allCols.GetByIndex(j)
		newPks[i] = pkCol
		newPkTags[i] = pkCol.Tag
	}
	si.pkCols = NewColCollection(newPks...)
	return si.indexCollection.SetPks(newPkTags)
}

func (si *schemaImpl) String() string {
	var b strings.Builder
	writeColFn := func(tag uint64, col Column) (stop bool, err error) {
		b.WriteString("tag: ")
		b.WriteString(strconv.FormatUint(tag, 10))
		b.WriteString(", name: ")
		b.WriteString(col.Name)
		b.WriteString(", type: ")
		b.WriteString(col.KindString())
		b.WriteString(",\n")
		return false, nil
	}
	b.WriteString("pkCols: [")
	err := si.pkCols.Iter(writeColFn)

	if err != nil {
		return err.Error()
	}

	b.WriteString("]\nnonPkCols: [")
	err = si.nonPKCols.Iter(writeColFn)

	if err != nil {
		return err.Error()
	}

	b.WriteString("]")
	return b.String()
}

func (si *schemaImpl) Indexes() IndexCollection {
	return si.indexCollection
}

func (si *schemaImpl) Checks() CheckCollection {
	return si.checkCollection
}

func (si schemaImpl) AddColumn(newCol Column, order *ColumnOrder) (Schema, error) {
	if newCol.IsPartOfPK {
		return nil, fmt.Errorf("cannot add a column with that is a primary key: %s", newCol.Name)
	}

	// preserve the primary key column names in their original order, which we'll need at the end
	keyCols := make([]string, len(si.pkOrdinals))
	for i, ordinal := range si.pkOrdinals {
		keyCols[i] = si.allCols.GetByIndex(ordinal).Name
	}

	var newCols []Column
	var pkCols []Column
	var nonPkCols []Column

	if order != nil && order.First {
		newCols = append(newCols, newCol)
		nonPkCols = append(nonPkCols, newCol)
	}

	for _, col := range si.GetAllCols().GetColumns() {
		newCols = append(newCols, col)
		if col.IsPartOfPK {
			pkCols = append(pkCols, col)
		} else {
			nonPkCols = append(nonPkCols, col)
		}

		if order != nil && order.AfterColumn == col.Name {
			newCols = append(newCols, newCol)
			nonPkCols = append(nonPkCols, newCol)
		}
	}

	if order == nil {
		newCols = append(newCols, newCol)
		nonPkCols = append(nonPkCols, newCol)
	}

	collection := NewColCollection(newCols...)
	si.allCols = collection
	si.pkCols = NewColCollection(pkCols...)
	si.nonPKCols = NewColCollection(nonPkCols...)

	// This must be done after we have set the new column order
	si.pkOrdinals = primaryKeyOrdinals(&si, keyCols)

	err := ValidateForInsert(collection)
	if err != nil {
		return nil, err
	}

	return &si, nil
}

// GetMapDescriptors implements the Schema interface.
func (si *schemaImpl) GetMapDescriptors(vs val.ValueStore) (keyDesc, valueDesc val.TupleDesc) {
	keyDesc = si.GetKeyDescriptor(vs)
	valueDesc = si.GetValueDescriptor(vs)
	return
}

// GetKeyDescriptor implements the Schema interface.
func (si *schemaImpl) GetKeyDescriptor(vs val.ValueStore) val.TupleDesc {
	return si.getKeyColumnsDescriptor(vs, true)
}

// GetKeyDescriptorWithNoConversion implements the Schema interface.
func (si *schemaImpl) GetKeyDescriptorWithNoConversion(vs val.ValueStore) val.TupleDesc {
	return si.getKeyColumnsDescriptor(vs, false)
}

func (si *schemaImpl) getKeyColumnsDescriptor(vs val.ValueStore, convertAddressColumns bool) val.TupleDesc {
	if IsKeyless(si) {
		return val.KeylessTupleDesc
	}

	contentHashedFields := make(map[uint64]struct{})
	for _, tag := range si.contentHashedFields {
		contentHashedFields[tag] = struct{}{}
	}

	var tt []val.Type
	var handlers []val.TupleTypeHandler
	useCollations := false // We only use collations if a string exists
	var collations []sql.CollationID
	_ = si.GetPKCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		sqlType := col.TypeInfo.ToSqlType()
		queryType := sqlType.Type()
		var t val.Type
		var handler val.TupleTypeHandler

		_, contentHashedField := contentHashedFields[tag]
		extendedType, isExtendedType := sqlType.(gmstypes.ExtendedType)

		if isExtendedType {
			encoding := EncodingFromSqlType(sqlType)
			t = val.Type{
				Enc:      val.Encoding(encoding),
				Nullable: columnMissingNotNullConstraint(col),
			}
			if encoding == serial.EncodingExtended {
				handler = extendedType
			} else {
				handler = val.NewExtendedAddressTypeHandler(vs, extendedType)
			}
		} else {
			if convertAddressColumns && !contentHashedField && queryType == query.Type_BLOB {
				t = val.Type{
					Enc:      val.Encoding(EncodingFromQueryType(query.Type_VARBINARY)),
					Nullable: columnMissingNotNullConstraint(col),
				}
			} else if convertAddressColumns && !contentHashedField && queryType == query.Type_TEXT {
				t = val.Type{
					Enc:      val.Encoding(EncodingFromQueryType(query.Type_VARCHAR)),
					Nullable: columnMissingNotNullConstraint(col),
				}
			} else if convertAddressColumns && !contentHashedField && queryType == query.Type_GEOMETRY {
				t = val.Type{
					Enc:      val.Encoding(serial.EncodingCell),
					Nullable: columnMissingNotNullConstraint(col),
				}
			} else {
				t = val.Type{
					Enc:      val.Encoding(EncodingFromSqlType(sqlType)),
					Nullable: columnMissingNotNullConstraint(col),
				}
			}
		}

		tt = append(tt, t)
		stringType, isStringType := sqlType.(sql.StringType)
		if isStringType && (queryType == query.Type_CHAR || queryType == query.Type_VARCHAR || queryType == query.Type_TEXT) {
			useCollations = true
			collations = append(collations, stringType.Collation())
		} else {
			collations = append(collations, sql.Collation_Unspecified)
		}

		handlers = append(handlers, handler)

		return
	})

	if useCollations {
		if len(collations) != len(tt) {
			panic(fmt.Errorf("cannot create tuple descriptor from %d collations and %d types", len(collations), len(tt)))
		}
		cmp := CollationTupleComparator{Collations: collations}
		return val.NewTupleDescriptorWithArgs(val.TupleDescriptorArgs{Comparator: cmp, Handlers: handlers}, tt...)
	} else {
		return val.NewTupleDescriptorWithArgs(val.TupleDescriptorArgs{Handlers: handlers}, tt...)
	}
}

// GetValueDescriptor implements the Schema interface.
func (si *schemaImpl) GetValueDescriptor(vs val.ValueStore) val.TupleDesc {
	var tt []val.Type
	var handlers []val.TupleTypeHandler
	var collations []sql.CollationID
	if IsKeyless(si) {
		tt = []val.Type{val.KeylessCardType}
		handlers = []val.TupleTypeHandler{nil}
		collations = []sql.CollationID{sql.Collation_Unspecified}
	}

	useCollations := false // We only use collations if a string exists
	_ = si.GetNonPKCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		if col.Virtual {
			return
		}

		sqlType := col.TypeInfo.ToSqlType()
		encoding := EncodingFromSqlType(sqlType)
		queryType := sqlType.Type()
		tt = append(tt, val.Type{
			Enc:      val.Encoding(encoding),
			Nullable: col.IsNullable(),
		})
		if queryType == query.Type_CHAR || queryType == query.Type_VARCHAR {
			useCollations = true
			collations = append(collations, sqlType.(sql.StringType).Collation())
		} else {
			collations = append(collations, sql.Collation_Unspecified)
		}

		if extendedType, ok := sqlType.(gmstypes.ExtendedType); ok {
			if encoding == serial.EncodingExtendedAddr {
				handlers = append(handlers, val.NewExtendedAddressTypeHandler(vs, extendedType))
			}
			handlers = append(handlers, extendedType)
		} else {
			handlers = append(handlers, nil)
		}
		return
	})

	if useCollations {
		if len(collations) != len(tt) {
			panic(fmt.Errorf("cannot create tuple descriptor from %d collations and %d types", len(collations), len(tt)))
		}
		cmp := CollationTupleComparator{Collations: collations}
		return val.NewTupleDescriptorWithArgs(val.TupleDescriptorArgs{Comparator: cmp, Handlers: handlers}, tt...)
	} else {
		return val.NewTupleDescriptorWithArgs(val.TupleDescriptorArgs{Handlers: handlers}, tt...)
	}
}

// GetCollation implements the Schema interface.
func (si *schemaImpl) GetCollation() Collation {
	// Schemas made before this change (and invalid schemas) will contain unspecified, so we'll the inherent collation
	// instead (as that matches their behavior).
	if si.collation == Collation_Unspecified {
		return Collation_utf8mb4_0900_bin
	}
	return si.collation
}

// SetCollation implements the Schema interface.
func (si *schemaImpl) SetCollation(collation Collation) {
	// Schemas made before this change may try to set this to unspecified, so we'll set it to the inherent collation.
	if collation == Collation_Unspecified {
		si.collation = Collation_utf8mb4_0900_bin
	} else {
		si.collation = collation
	}
}

// indexOf returns the index of the given column in the overall schema
func (si *schemaImpl) indexOf(colName string) int {
	i, idx := 0, -1
	si.allCols.Iter(func(tag uint64, col Column) (stop bool, err error) {
		if strings.EqualFold(col.Name, colName) {
			idx = i
			return true, nil
		}
		i++
		return false, nil
	})

	return idx
}

// primaryKeyOrdinals returns the primary key ordinals for the schema given and the column names of the key columns
// given.
func primaryKeyOrdinals(sch *schemaImpl, keyCols []string) []int {
	ordinals := make([]int, len(keyCols))
	for i, colName := range keyCols {
		ordinals[i] = sch.indexOf(colName)
	}

	return ordinals
}

func columnMissingNotNullConstraint(col Column) bool {
	for _, cnst := range col.Constraints {
		if cnst.GetConstraintType() == NotNullConstraintType {
			return false
		}
	}
	return true
}

// Copy creates a copy of this schema safe to be edited independently. Some members, like column collections, are
// immutable and don't need to be copied. Others, like index and check collections, must be copied.
// We do this because it's cheaper to copy a schema than to deserialize one.
func (si schemaImpl) Copy() Schema {
	pkOrds := make([]int, len(si.pkOrdinals))
	copy(pkOrds, si.pkOrdinals)

	si.indexCollection = si.indexCollection.Copy()
	si.checkCollection = si.checkCollection.Copy()

	return &si
}
