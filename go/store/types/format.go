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

package types

import (
	"errors"

	"github.com/dolthub/dolt/go/store/constants"
)

type NomsBinFormat struct {
	tag *formatTag
}

type formatTag struct {
	// Can't be zero size of allocations are not unique.
	furp byte
}

var formatTag_DOLT = &formatTag{}
var Format_DOLT = &NomsBinFormat{formatTag_DOLT}

func GetFormatForVersionString(s string) (*NomsBinFormat, error) {
	if s == constants.FormatDoltString {
		return Format_DOLT, nil
	} else {
		return nil, errors.New("unsupported ChunkStore version " + s)
	}
}

func (nbf *NomsBinFormat) VersionString() string {
	if nbf.tag == formatTag_DOLT {
		return constants.FormatDoltString
	} else {
		panic("unrecognized NomsBinFormat tag value")
	}
}
