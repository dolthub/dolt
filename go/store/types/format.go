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

package types

import (
	"errors"

	"github.com/liquidata-inc/dolt/go/store/constants"
)

func init() {
	nbf, err := GetFormatForVersionString(constants.FormatDefaultString)
	if err != nil {
		panic("unrecognized value for DOLT_DEFAULT_BIN_FORMAT " + constants.FormatDefaultString)
	}
	Format_Default = nbf
}

type NomsBinFormat struct {
	tag *formatTag
}

type formatTag struct{}

var formatTag_7_18 *formatTag = nil
var formatTag_LD_1 = &formatTag{}

var Format_7_18 = &NomsBinFormat{}
var Format_LD_1 = &NomsBinFormat{formatTag_LD_1}

var Format_Default *NomsBinFormat

func isFormat_7_18(nbf *NomsBinFormat) bool {
	return nbf.tag == formatTag_7_18
}

func GetFormatForVersionString(s string) (*NomsBinFormat, error) {
	if s == constants.Format718String {
		return Format_7_18, nil
	} else if s == constants.FormatLD1String {
		return Format_LD_1, nil
	} else {
		return nil, errors.New("unsupported ChunkStore version " + s)
	}
}

func (nbf *NomsBinFormat) VersionString() string {
	if nbf.tag == formatTag_7_18 {
		return constants.Format718String
	} else if nbf.tag == formatTag_LD_1 {
		return constants.FormatLD1String
	} else {
		panic("unrecognized NomsBinFormat tag value")
	}
}
