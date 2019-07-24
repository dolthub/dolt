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

package xlsx

import "github.com/liquidata-inc/dolt/go/libraries/doltcore/row"

type XLSXFileInfo struct {
	Rows []row.Row
}

func NewXLSXInfo() *XLSXFileInfo {
	return &XLSXFileInfo{nil}
}

func (info *XLSXFileInfo) SetRows(rows []row.Row) *XLSXFileInfo {
	info.Rows = rows
	return info
}
