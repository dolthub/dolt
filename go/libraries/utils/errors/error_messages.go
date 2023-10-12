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

package errors

import (
	"fmt"
	"strings"
)

// CreateUndropErrorMessage returns a string to be used in errors returned from attempts to undrop a database.
// The error message string either states that no databases are available to be undropped, or it lists
// the names of the databases that are available to be undropped.
func CreateUndropErrorMessage(availableDatabases []string) string {
	if len(availableDatabases) == 0 {
		return "there are no databases currently available to be undropped"
	} else {
		return fmt.Sprintf("available databases that can be undropped: %s", strings.Join(availableDatabases, ", "))
	}
}
