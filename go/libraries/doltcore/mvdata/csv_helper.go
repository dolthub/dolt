// Copyright 2023 Dolthub, Inc.
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

package mvdata

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
)

// CreateCSVInfo creates a CSVInfo object based on the provided options.
// This is a helper function that extracts and processes CSV-related options
// from the generic options interface.
func CreateCSVInfo(opts interface{}, defaultDelim string) *csv.CSVFileInfo {
	hasHeaderLine := true
	var columns []string
	delim := defaultDelim

	if opts != nil {
		csvOpts, _ := opts.(CsvOptions)

		if len(csvOpts.Delim) != 0 {
			delim = csvOpts.Delim
		}

		if csvOpts.NoHeader {
			hasHeaderLine = false
		}

		if len(csvOpts.Columns) > 0 {
			columns = csvOpts.Columns
		}
	}

	csvInfo := csv.NewCSVInfo().SetDelim(delim).SetHasHeaderLine(hasHeaderLine)
	if len(columns) > 0 {
		csvInfo = csvInfo.SetColumns(columns)
	}

	return csvInfo
}
