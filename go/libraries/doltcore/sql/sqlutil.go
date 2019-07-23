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

package sql

import (
	"errors"
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
)

const UnknownTableErrFmt = "Unknown table: '%v'"
const AmbiguousTableErrFmt = "Ambiguous table: '%v'"
const UnknownColumnErrFmt = "Unknown column: '%v'"
const AmbiguousColumnErrFmt = "Ambiguous column: '%v'"

// Turns a node to a string
func nodeToString(node sqlparser.SQLNode) string {
	buffer := sqlparser.NewTrackedBuffer(nil)
	node.Format(buffer)
	return buffer.String()
}

// Returns an error with the format string and arguments given.
func errFmt(fmtMsg string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(fmtMsg, args...))
}