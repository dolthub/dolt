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
	"os"
	"sync"

	"github.com/dolthub/dolt/go/store/constants"
)

func init() {
	// check for new format feature flag
	if v, ok := os.LookupEnv(doltFormatFeatureFlag); ok && v != "" {
		constants.FormatDefaultString = constants.FormatDolt1String
	}

	nbf, err := GetFormatForVersionString(constants.FormatDefaultString)
	if err != nil {
		panic("unrecognized value for DOLT_DEFAULT_BIN_FORMAT " + constants.FormatDefaultString)
	}
	nbfLock.Lock()
	defer nbfLock.Unlock()
	Format_Default = nbf
}

const (
	doltFormatFeatureFlag = "DOLT_FORMAT_FEATURE_FLAG"
)

type NomsBinFormat struct {
	tag *formatTag
}

type formatTag struct {
	furp byte
}

var formatTag_7_18 *formatTag = nil
var formatTag_LD_1 = &formatTag{}
var formatTag_DOLT_1 = &formatTag{}

var Format_7_18 = &NomsBinFormat{}
var Format_LD_1 = &NomsBinFormat{formatTag_LD_1}
var Format_DOLT_1 = &NomsBinFormat{formatTag_DOLT_1}

var nbfLock = &sync.Mutex{}
var Format_Default *NomsBinFormat

var emptyTuples = make(map[*NomsBinFormat]Tuple)

func init() {
	emptyTuples[Format_7_18], _ = NewTuple(Format_7_18)
	emptyTuples[Format_LD_1], _ = NewTuple(Format_LD_1)
}

func isFormat_7_18(nbf *NomsBinFormat) bool {
	return nbf.tag == formatTag_7_18
}

var ErrUnsupportedFormat = errors.New("operation not supported for format '__DOLT_1__' ")

func IsFormat_DOLT_1(nbf *NomsBinFormat) bool {
	return nbf.tag == formatTag_DOLT_1
}

func TestFormatDolt1(cb func() error) error {
	nbfLock.Lock()
	defer nbfLock.Unlock()

	var stash *NomsBinFormat
	stash, Format_Default = Format_Default, Format_DOLT_1
	defer func() {
		Format_Default = stash
	}()

	return cb()
}

func GetFormatForVersionString(s string) (*NomsBinFormat, error) {
	if s == constants.Format718String {
		return Format_7_18, nil
	} else if s == constants.FormatLD1String {
		return Format_LD_1, nil
	} else if s == constants.FormatDolt1String {
		return Format_DOLT_1, nil
	} else {
		return nil, errors.New("unsupported ChunkStore version " + s)
	}
}

func (nbf *NomsBinFormat) VersionString() string {
	if nbf.tag == formatTag_7_18 {
		return constants.Format718String
	} else if nbf.tag == formatTag_LD_1 {
		return constants.FormatLD1String
	} else if nbf.tag == formatTag_DOLT_1 {
		return constants.FormatDolt1String
	} else {
		panic("unrecognized NomsBinFormat tag value")
	}
}
