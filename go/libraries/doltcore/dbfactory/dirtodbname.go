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

package dbfactory

import (
	"os"
	"strings"
	"unicode"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
)

// DirToDBName takes the physical directory name, |dirName|, and replaces any unsupported characters to create a
// valid logical database name. For example, spaces are replaced with underscores.
func DirToDBName(dirName string) string {
	// this environment variable is used whether to replace hyphen and space characters in the database name with underscores.
	if os.Getenv(dconfig.EnvDbNameReplace) == "" {
		return dirName
	}

	dbName := strings.TrimSpace(dirName)
	dbName = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || r == '-' {
			return '_'
		}
		return r
	}, dbName)

	newDBName := strings.ReplaceAll(dbName, "__", "_")

	for dbName != newDBName {
		dbName = newDBName
		newDBName = strings.ReplaceAll(dbName, "__", "_")
	}

	return dbName
}
