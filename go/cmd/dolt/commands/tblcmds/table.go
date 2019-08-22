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

import (
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "import", Desc: "Creates, overwrites, or updates a table from the data in a file.", Func: Import, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_IMPORT},
	{Name: "export", Desc: "Export a table to a file.", Func: Export, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_EXPORT},
	{Name: "create", Desc: "Creates or overwrite an existing table with an empty table.", Func: Create, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_CREATE},
	{Name: "rm", Desc: "Deletes a table", Func: Rm, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_RM},
	{Name: "mv", Desc: "Moves a table", Func: Mv, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_MV},
	{Name: "cp", Desc: "Copies a table", Func: Cp, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_CP},
	{Name: "select", Desc: "Print a selection of a table.", Func: Select, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_SELECT},
	{Name: "put-row", Desc: "Add a row to a table.", Func: PutRow, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_PUT_ROW},
	{Name: "rm-row", Desc: "Remove a row from a table.", Func: RmRow, ReqRepo: true, EventType: eventsapi.ClientEventType_TABLE_RM_ROW},
})
