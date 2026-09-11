// Copyright 2024 Dolthub, Inc.
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

package resolve

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// UseSearchPath is a global variable that determines whether or not to use the search path when resolving table names.
// Currently used by Doltgres
var UseSearchPath = false

// SearchPath returns all the schemas in the search_path setting, with elements like "$user" expanded
func SearchPath(ctx *sql.Context) ([]string, error) {
	searchPathVar, err := ctx.GetSessionVariable(ctx, "search_path")
	if err != nil {
		return nil, err
	}

	pathElems := SplitSearchPath(searchPathVar.(string))
	path := make([]string, len(pathElems))
	for i, pathElem := range pathElems {
		path[i] = normalizeSearchPathSchema(ctx, pathElem)
	}

	return path, nil
}

// SplitSearchPath splits a search_path setting into its schema names. It follows the same rules Postgres
// applies---elements are separated by commas, whitespace around an element is insignificant, an element
// may be double quoted, which preserves its case and lets it hold characters that are not valid in a
// bare identifier (including a comma), a doubled double quote within a quoted element stands for one
// literal quote. A bare element is lower cased, the same as an unquoted identifier elsewhere in SQL.
// An empty setting names no schemas at all, which is distinct from the single empty element that `""`
// names.
func SplitSearchPath(searchPath string) []string {
	if len(strings.Trim(searchPath, searchPathSpace)) == 0 {
		return nil
	}

	var elems []string
	for pos := 0; pos >= 0; {
		var elem string
		elem, pos = readSearchPathElem(searchPath, pos)
		elems = append(elems, elem)
	}
	return elems
}

// searchPathSpace holds the characters Postgres's scanner treats as whitespace.
const searchPathSpace = " \t\n\r\f"

// readSearchPathElem reads the search_path element beginning at pos, returning the read schema name
// along with the position the following element begins at, or -1 once the element read was the last
// one.
//
// There is no error reporting on this path. A malformed element is read as far as it makes sense: an
// unterminated quote runs to the end of the value, and anything between a closing quote and the
// following comma is discarded.
func readSearchPathElem(searchPath string, pos int) (string, int) {
	// Skip starting whitespace.
	for pos < len(searchPath) && isSearchPathSpace(searchPath[pos]) {
		pos++
	}

	// A bare element ends at the next comma, and the whitespace around it is not part of the name
	if pos == len(searchPath) || searchPath[pos] != '"' {
		if comma := strings.IndexByte(searchPath[pos:], ','); comma >= 0 {
			return strings.ToLower(strings.Trim(searchPath[pos:pos+comma], searchPathSpace)), pos + comma + 1
		}
		return strings.ToLower(strings.Trim(searchPath[pos:], searchPathSpace)), -1
	}

	// Starting one past the " literal we just saw.
	var elem strings.Builder
	for pos++; pos < len(searchPath); pos++ {
		if searchPath[pos] != '"' {
			elem.WriteByte(searchPath[pos])
			continue
		}
		// Is it a double quote?
		if pos+1 < len(searchPath) && searchPath[pos+1] == '"' {
			elem.WriteByte('"')
			pos++
			continue
		}
		// Found an end quote.
		pos++
		break
	}
	if comma := strings.IndexByte(searchPath[pos:], ','); comma >= 0 {
		return elem.String(), pos + comma + 1
	}
	return elem.String(), -1
}

// isSearchPathSpace returns whether the byte is whitespace as far as splitting a search_path setting is concerned.
func isSearchPathSpace(c byte) bool {
	return strings.IndexByte(searchPathSpace, c) >= 0
}

// normalizeSearchPathSchema resolves the "$user" element, which stands for a schema named after the session's user,
// to that user's name.
func normalizeSearchPathSchema(ctx *sql.Context, schemaName string) string {
	if schemaName == "$user" {
		client := ctx.Session.Client()
		return client.User
	}
	return schemaName
}

// FirstExistingSchemaOnSearchPath returns the first schema in the search path that exists in the database.
func FirstExistingSchemaOnSearchPath(ctx *sql.Context, root doltdb.RootValue) (string, error) {
	schemas, err := SearchPath(ctx)
	if err != nil {
		return "", err
	}

	schemaName := ""
	for _, s := range schemas {
		var exists bool
		schemaName, exists, err = doltdb.ResolveDatabaseSchema(ctx, root, s)
		if err != nil {
			return "", err
		}

		if exists {
			break
		}
	}

	// No existing schema found in the search_path and none specified in the statement means we can't create the table
	if schemaName == "" {
		return "", sql.ErrDatabaseNoDatabaseSchemaSelectedCreate.New()
	}

	return schemaName, nil
}

// IsDoltgresSystemTable returns whether a table is a doltgres system table or not
func IsDoltgresSystemTable(ctx *sql.Context, tableName doltdb.TableName, root doltdb.RootValue) (bool, error) {
	if doltdb.IsSystemTable(tableName) {
		return true, nil
	}
	if !UseSearchPath || tableName.Schema != "" {
		return false, nil
	}

	schemasToSearch, err := SearchPath(ctx)
	if err != nil {
		return false, nil
	}
	for _, schemaName := range schemasToSearch {
		if schemaName == doltdb.DoltNamespace {
			return true, nil
		}

		_, ok, err := root.ResolveTableName(ctx, doltdb.TableName{Name: tableName.Name, Schema: schemaName})
		if err != nil {
			return false, err
		}
		if ok {
			return false, nil
		}
	}

	return false, nil
}
