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
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
)

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "add-column", Desc: "Adds a column to specified table's schema.", Func: AddColumn, ReqRepo: true, EventType: eventsapi.ClientEventType_SCHEMA},
	{Name: "drop-column", Desc: "Removes a column of the specified table.", Func: DropColumn, ReqRepo: true, EventType: eventsapi.ClientEventType_SCHEMA},
	{Name: "export", Desc: "Exports a table's schema.", Func: Export, ReqRepo: true, EventType: eventsapi.ClientEventType_SCHEMA},
	//{Name: "import", Desc: "Creates a new table with an inferred schema.", Func: Import, ReqRepo: true, EventType: eventsapi.ClientEventType_SCHEMA},
	{Name: "rename-column", Desc: "Renames a column of the specified table.", Func: RenameColumn, ReqRepo: true, EventType: eventsapi.ClientEventType_SCHEMA},
	{Name: "show", Desc: "Shows the schema of one or more tables.", Func: Show, ReqRepo: true, EventType: eventsapi.ClientEventType_SCHEMA},
})
