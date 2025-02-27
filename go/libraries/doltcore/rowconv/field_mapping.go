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

package rowconv

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// ErrMappingFileRead is an error returned when a mapping file cannot be read
var ErrMappingFileRead = errors.New("error reading mapping file")

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

// NameMapper is a simple interface for mapping a string to another string
type NameMapper map[string]string

// Map maps a string to another string.  If a string is not in the mapping ok will be false, otherwise it is true.
func (nm NameMapper) Map(str string) string {
	v, ok := nm[str]
	if ok {
		return v
	}
	return str
}

// PreImage searches the NameMapper for the string that maps to str, returns str otherwise
func (nm NameMapper) PreImage(str string) string {
	for pre, post := range nm {
		if post == str {
			return pre
		}
	}
	return str
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

// NewFieldMapping creates a FieldMapping from a source schema, a destination schema, and a map from tags in the source
// schema to tags in the dest schema.
func NewFieldMapping(srcSch, destSch schema.Schema, srcTagToDestTag map[uint64]uint64) (*FieldMapping, error) {
	destCols := destSch.GetAllCols()

	for srcTag, destTag := range srcTagToDestTag {
		_, destOk := destCols.GetByTag(destTag)

		if !destOk {
			return nil, &BadMappingErr{"src tag:" + strconv.FormatUint(srcTag, 10), "dest tag:" + strconv.FormatUint(destTag, 10)}
		}
	}

	if len(srcTagToDestTag) == 0 {
		return nil, ErrEmptyMapping
	}

	return &FieldMapping{srcSch, destSch, srcTagToDestTag}, nil
}

// TagMapping takes a source schema and a destination schema and maps all columns which have a matching tag in the
// source and destination schemas.
func TagMapping(srcSch, destSch schema.Schema) (*FieldMapping, error) {
	successes := 0
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	srcToDest := make(map[uint64]uint64, destCols.Size())
	err := destCols.Iter(func(destTag uint64, col schema.Column) (stop bool, err error) {
		srcCol, ok := srcCols.GetByTag(destTag)

		if ok {
			srcToDest[srcCol.Tag] = destTag
			successes++
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(srcSch, destSch, srcToDest)
}

// NameMapping takes a source schema and a destination schema and maps all columns which have a matching name in the
// source and destination schemas.
func NameMapping(srcSch, destSch schema.Schema, nameMapper NameMapper) (*FieldMapping, error) {
	successes := 0
	srcCols := srcSch.GetAllCols()
	destCols := destSch.GetAllCols()

	srcToDest := make(map[uint64]uint64, destCols.Size())
	err := srcCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		mn := nameMapper.Map(col.Name)
		outCol, ok := destCols.GetByName(mn)

		if ok {
			srcToDest[tag] = outCol.Tag
			successes++
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(srcSch, destSch, srcToDest)
}

// NameMapperFromFile reads a JSON file containing a name mapping and returns a NameMapper.
func NameMapperFromFile(mappingFile string, FS filesys.ReadableFS) (NameMapper, error) {
	var nm NameMapper

	if mappingFile == "" {
		// identity mapper
		return make(NameMapper), nil
	}

	if fileExists, _ := FS.Exists(mappingFile); !fileExists {
		return nil, errhand.BuildDError("error: '%s' does not exist.", mappingFile).Build()
	}

	err := filesys.UnmarshalJSONFile(FS, mappingFile, &nm)

	if err != nil {
		return nil, errhand.BuildDError("%s", ErrMappingFileRead.Error()).AddCause(err).Build()
	}

	return nm, nil
}

// TagMappingByTagAndName takes a source schema and a destination schema and maps
// pks by tag and non-pks by name.
func TagMappingByTagAndName(srcSch, destSch schema.Schema) (*FieldMapping, error) {
	srcToDest := make(map[uint64]uint64, destSch.GetAllCols().Size())

	keyMap, valMap, err := schema.MapSchemaBasedOnTagAndName(srcSch, destSch)
	if err != nil {
		return nil, err
	}

	var successes int
	for i, j := range keyMap {
		if j == -1 {
			continue
		}
		srcTag := srcSch.GetPKCols().GetByIndex(i).Tag
		dstTag := destSch.GetPKCols().GetByIndex(j).Tag
		srcToDest[srcTag] = dstTag
		successes++
	}
	for i, j := range valMap {
		if j == -1 {
			continue
		}
		srcTag := srcSch.GetNonPKCols().GetByIndex(i).Tag
		dstTag := destSch.GetNonPKCols().GetByIndex(j).Tag
		srcToDest[srcTag] = dstTag
		successes++
	}

	if successes == 0 {
		return nil, ErrEmptyMapping
	}

	return NewFieldMapping(srcSch, destSch, srcToDest)
}
