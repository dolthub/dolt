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

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// ErrMappingFileRead is an error returned when a mapping file cannot be read
var ErrMappingFileRead = errors.New("error reading mapping file")

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
