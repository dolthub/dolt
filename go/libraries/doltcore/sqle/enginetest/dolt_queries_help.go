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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltHelpScripts = []queries.ScriptTest{
	{
		Name:        "dolt_help arguments are valid json",
		SetUpScript: []string{},
		Query:       "select * from dolt_help where json_valid(arguments)=false;",
		Expected:    []sql.Row{},
	},
	{
		Name: "dolt_help synopsis remains the same after multiple queries",
		SetUpScript: []string{
			"set @InitialSynopsis=(select synopsis from dolt_help where name='dolt_branch')",
		},
		Query:    "select * from dolt_help where name='dolt_branch' and synopsis!=@InitialSynopsis",
		Expected: []sql.Row{},
	},
}
