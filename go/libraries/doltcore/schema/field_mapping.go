package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
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
	SrcSch    *Schema
	DestSch   *Schema
	DestToSrc []int
}

func NewFieldMapping(inSch, outSch *Schema, destToSrc []int) (*FieldMapping, error) {
	if outSch.GetPKIndex() != -1 && destToSrc[outSch.GetPKIndex()] == -1 {
		return nil, ErrPrimaryKeyNotMapped
	}

	return &FieldMapping{inSch, outSch, destToSrc}, nil
}

func NewFieldMappingFromNameMap(inSch, outSch *Schema, inNameToOutName map[string]string) (*FieldMapping, error) {
	destToSrc := make([]int, outSch.NumFields())
	for i := 0; i < outSch.NumFields(); i++ {
		destToSrc[i] = -1
	}

	successes := 0
	for k, v := range inNameToOutName {
		inIndex := inSch.GetFieldIndex(k)
		outIndex := outSch.GetFieldIndex(v)

		if inIndex == -1 || outIndex == -1 {
			return nil, &BadMappingErr{k, v}
		}

		destToSrc[outIndex] = inIndex
		successes++
	}

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(inSch, outSch, destToSrc)
}

func NewInferredMapping(inSch, outSch *Schema) (*FieldMapping, error) {
	successes := 0
	destToSrc := make([]int, outSch.NumFields())
	for i := 0; i < outSch.NumFields(); i++ {
		outFld := outSch.GetField(i)

		fldIdx := inSch.GetFieldIndex(outFld.NameStr())
		destToSrc[i] = fldIdx

		if fldIdx != -1 {
			successes++
		}
	}

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(inSch, outSch, destToSrc)
}

func MappingFromFile(mappingFile string, fs filesys.ReadableFS, inSch, outSch *Schema) (*FieldMapping, error) {
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

/*
 */
