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

package tblcmds

import "github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "import", Desc: "Creates, overwrites, or updates a table from the data in a file.", Func: Import, ReqRepo: true},
	{Name: "export", Desc: "Export a table to a file.", Func: Export, ReqRepo: true},
	{Name: "create", Desc: "Creates or overwrite an existing table with an empty table.", Func: Create, ReqRepo: true},
	{Name: "rm", Desc: "Deletes a table", Func: Rm, ReqRepo: true},
	{Name: "mv", Desc: "Moves a table", Func: Mv, ReqRepo: true},
	{Name: "cp", Desc: "Copies a table", Func: Cp, ReqRepo: true},
	{Name: "select", Desc: "Print a selection of a table.", Func: Select, ReqRepo: true},
	{Name: "put-row", Desc: "Add a row to a table.", Func: PutRow, ReqRepo: true},
	{Name: "rm-row", Desc: "Remove a row from a table.", Func: RmRow, ReqRepo: true},
})
