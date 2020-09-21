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

package schcmds

import (
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

var Commands = cli.NewSubCommandHandler("schema", "Commands for showing and importing table schemas.", []cli.Command{
	ExportCmd{},
	ImportCmd{},
	ShowCmd{},
	TagsCmd{},
})

// ValidateTableNameForCreate validates the given table name for creation as a user table, returning an error if the
// table name is not valid.
func ValidateTableNameForCreate(tableName string) errhand.VerboseError {
	if !doltdb.IsValidTableName(tableName) {
		return errhand.BuildDError("'%s' is not a valid table name\ntable names must match the regular expression: %s",
			tableName, doltdb.TableNameRegexStr).Build()
	} else if doltdb.HasDoltPrefix(tableName) {
		return errhand.BuildDError("'%s' is not a valid table name\ntable names beginning with dolt_ are reserved for internal use", tableName).Build()
	}
	return nil
}
