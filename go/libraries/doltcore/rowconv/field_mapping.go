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

var ErrPrimaryKeyNotMapped = errors.New("primary key not mapped")
var ErrMappingFileRead = errors.New("error reading mapping file")
var ErrUnmarshallingMapping = errors.New("error unmarshalling mapping")
var ErrEmptyMapping = errors.New("empty mapping error")

type BadMappingErr struct {
	srcField  string
	destField string
}

func (err *BadMappingErr) Error() string {
	return fmt.Sprintf("Mapping file attempted to map %s to %s, but one or both of those fields are unknown.", err.srcField, err.destField)
}

var ErrBadMapping = errors.New("error bad mapping")

func IsBadMappingErr(err error) bool {
	_, ok := err.(*BadMappingErr)
	return ok
}

type FieldMapping struct {
	SrcSch    schema.Schema
	DestSch   schema.Schema
	SrcToDest map[uint64]uint64
}

func NewFieldMapping(inSch, outSch schema.Schema, srcTagToDestTag map[uint64]uint64) (*FieldMapping, error) {
	inCols := inSch.GetAllCols()
	outCols := outSch.GetAllCols()

	for srcTag, destTag := range srcTagToDestTag {
		_, destOk := outCols.GetByTag(destTag)
		_, srcOk := inCols.GetByTag(srcTag)

		if !destOk || !srcOk {
			return nil, &BadMappingErr{"src tag:" + strconv.FormatUint(srcTag, 10), "dest tag:" + strconv.FormatUint(destTag, 10)}
		}
	}

	if len(srcTagToDestTag) == 0 {
		return nil, ErrEmptyMapping
	}

	return &FieldMapping{inSch, outSch, srcTagToDestTag}, nil
}

func NewFieldMappingFromNameMap(inSch, outSch schema.Schema, inNameToOutName map[string]string) (*FieldMapping, error) {
	inCols := inSch.GetAllCols()
	outCols := outSch.GetAllCols()
	srcToDest := make(map[uint64]uint64, len(inNameToOutName))

	for k, v := range inNameToOutName {
		inCol, inOk := inCols.GetByName(k)
		outCol, outOk := outCols.GetByName(v)

		if !inOk || !outOk {
			return nil, &BadMappingErr{k, v}
		}

		srcToDest[inCol.Tag] = outCol.Tag
	}

	return NewFieldMapping(inSch, outSch, srcToDest)
}

func NewInferredMapping(inSch, outSch schema.Schema) (*FieldMapping, error) {
	successes := 0
	inCols := inSch.GetAllCols()
	outCols := outSch.GetAllCols()

	srcToDest := make(map[uint64]uint64, outCols.Size())
	outCols.ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		inCol, ok := inCols.GetByTag(tag)

		if ok {
			srcToDest[inCol.Tag] = tag
			successes++
		}

		return false
	})

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(inSch, outSch, srcToDest)
}

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

func TypedToUntypedMapping(sch schema.Schema) *FieldMapping {
	untypedSch := untyped.UntypeSchema(sch)

	identityMap := make(map[uint64]uint64)
	sch.GetAllCols().ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		identityMap[tag] = tag
		return false
	})

	mapping, err := NewFieldMapping(sch, untypedSch, identityMap)

	if err != nil {
		panic(err)
	}

	return mapping
}
