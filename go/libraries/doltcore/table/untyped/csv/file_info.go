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

package csv

// CSVFileInfo describes a csv file
type CSVFileInfo struct {
	// Delim says which character is used as a field delimiter
	Delim string
	// HasHeaderLine says if the csv has a header line which contains the names of the columns
	HasHeaderLine bool
	// Columns can be provided if you no the columns and their order in the csv
	Columns []string
	// EscapeQuotes says whether quotes should be escaped when parsing the csv
	EscapeQuotes bool
}

// NewCSVInfo creates a new CSVInfo struct with default values
func NewCSVInfo() *CSVFileInfo {
	return &CSVFileInfo{",", true, nil, true}
}

// SetDelim sets the Delim member and returns the CSVFileInfo
func (info *CSVFileInfo) SetDelim(delim string) *CSVFileInfo {
	info.Delim = delim
	return info
}

// SetHasHeaderLine sets the HeaderLine member and returns the CSVFileInfo
func (info *CSVFileInfo) SetHasHeaderLine(hasHeaderLine bool) *CSVFileInfo {
	info.HasHeaderLine = hasHeaderLine
	return info
}

// SetColumns sets the Columns member and returns the CSVFileInfo
func (info *CSVFileInfo) SetColumns(columns []string) *CSVFileInfo {
	info.Columns = columns
	return info
}

// SetEscapeQuotes sets the EscapeQuotes member and returns the CSVFileInfo
func (info *CSVFileInfo) SetEscapeQuotes(escapeQuotes bool) *CSVFileInfo {
	info.EscapeQuotes = escapeQuotes
	return info
}
