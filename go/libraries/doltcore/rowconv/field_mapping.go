package rowconv

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"strconv"
)

// ErrMappingFileRead is an error returned when a mapping file cannot be read
var ErrMappingFileRead = errors.New("error reading mapping file")

// ErrUnmarshallingMapping is an error used when a mapping file cannot be converted from json
var ErrUnmarshallingMapping = errors.New("error unmarshalling mapping")

// ErrEmptyMapping is an error returned when the mapping is empty (No src columns, no destination columns)
var ErrEmptyMapping = errors.New("empty mapping error")

// BadMappingErr is a struct which implements the error interface and is used when there is an error with a mapping.
type BadMappingErr struct {
	srcField  string
	destField string
}

// String representing the BadMappingError
func (err *BadMappingErr) Error() string {
	return fmt.Sprintf("Mapping file attempted to map %s to %s, but one or both of those fields are unknown.", err.srcField, err.destField)
}

// IsBadMappingErr returns true if the error is a BadMappingErr
func IsBadMappingErr(err error) bool {
	_, ok := err.(*BadMappingErr)
	return ok
}

// FieldMapping defines a mapping from columns in a source schema to columns in a dest schema.
type FieldMapping struct {
	// SrcSch is the source schema being mapped from.
	SrcSch schema.Schema

	// DestSch is the destination schema being mapped to.
	DestSch schema.Schema

	// SrcToDest is a map from a tag in the source schema to a tag in the dest schema.
	SrcToDest map[uint64]uint64
}

func InvertMapping(fm *FieldMapping) *FieldMapping {
	invertedMap := make(map[uint64]uint64)

	for k, v := range fm.SrcToDest {
		invertedMap[v] = k
	}

	return &FieldMapping{
		SrcSch:    fm.DestSch,
		DestSch:   fm.SrcSch,
		SrcToDest: invertedMap,
	}
}

// NewFieldMapping creates a FieldMapping from a source schema, a destination schema, and a map from tags in the source
// schema to tags in the dest schema.
func NewFieldMapping(srcSch, destSch schema.Schema, srcTagToDestTag map[uint64]uint64) (*FieldMapping, error) {
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	for srcTag, destTag := range srcTagToDestTag {
		_, destOk := destCols.GetByTag(destTag)
		_, srcOk := srcCols.GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, &BadMappingErr{"src tag:" + strconv.FormatUint(srcTag, 10), "dest tag:" + strconv.FormatUint(destTag, 10)}
		}
	}

	if len(srcTagToDestTag) == 0 {
		return nil, ErrEmptyMapping
	}

	return &FieldMapping{srcSch, destSch, srcTagToDestTag}, nil
}

// NewFieldMappingFromNameMap creates a FieldMapping from a source schema, a destination schema, and a map from column
// names in the source schema to column names in the dest schema.
func NewFieldMappingFromNameMap(srcSch, destSch schema.Schema, inNameToOutName map[string]string) (*FieldMapping, error) {
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()
	srcToDest := make(map[uint64]uint64, len(inNameToOutName))

	for k, v := range inNameToOutName {
		inCol, inOk := srcCols.GetByName(k)
		outCol, outOk := destCols.GetByName(v)

		if !inOk || !outOk {
			return nil, &BadMappingErr{k, v}
		}

		srcToDest[inCol.Tag] = outCol.Tag
	}

	return NewFieldMapping(srcSch, destSch, srcToDest)
}

// Returns the identity mapping for the schema given.
func IdentityMapping(sch schema.Schema) *FieldMapping {
	fieldMapping, err := TagMapping(sch, sch)
	if err != nil {
		panic("Error creating identity mapping")
	}
	return fieldMapping
}

// TagMapping takes a source schema and a destination schema and maps all columns which have a matching tag in the
// source and destination schemas.
func TagMapping(srcSch, destSch schema.Schema) (*FieldMapping, error) {
	successes := 0
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	srcToDest := make(map[uint64]uint64, destCols.Size())
	destCols.Iter(func(destTag uint64, col schema.Column) (stop bool) {
		srcCol, ok := srcCols.GetByTag(destTag)

		if ok {
			srcToDest[srcCol.Tag] = destTag
			successes++
		}

		return false
	})

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(srcSch, destSch, srcToDest)
}

// NameMapping takes a source schema and a destination schema and maps all columns which have a matching name in the
// source and destination schemas.
func NameMapping(srcSch, destSch schema.Schema) (*FieldMapping, error) {
	successes := 0
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	srcToDest := make(map[uint64]uint64, destCols.Size())
	destCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		inCol, ok := srcCols.GetByName(col.Name)

		if ok {
			srcToDest[inCol.Tag] = tag
			successes++
		}

		return false
	})

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(srcSch, destSch, srcToDest)
}

// MappingFromFile reads a FieldMapping from a json file
func MappingFromFile(mappingFile string, fs filesys.ReadableFS, inSch, outSch schema.Schema) (*FieldMapping, error) {
	data, err := fs.ReadFile(mappingFile)

	if err != nil {
		return nil, ErrMappingFileRead
	}

	var inNameToOutName map[string]string
	err = json.Unmarshal(data, &inNameToOutName)

	if err != nil {
		return nil, ErrUnmarshallingMapping
	}

	return NewFieldMappingFromNameMap(inSch, outSch, inNameToOutName)
}

// TypedToUntypedMapping takes a schema and creates a mapping to an untyped schema with all the same columns.
func TypedToUntypedMapping(sch schema.Schema) *FieldMapping {
	untypedSch := untyped.UntypeSchema(sch)

	identityMap := make(map[uint64]uint64)
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		identityMap[tag] = tag
		return false
	})

	mapping, err := NewFieldMapping(sch, untypedSch, identityMap)

	if err != nil {
		panic(err)
	}

	return mapping
}
