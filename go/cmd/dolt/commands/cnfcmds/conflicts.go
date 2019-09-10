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

package cnfcmds

import (
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
)

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "cat", Desc: "Writes out the table conflicts.", Func: Cat, ReqRepo: true, EventType: eventsapi.ClientEventType_CONF_CAT},
	{Name: "resolve", Desc: "Removes rows from list of conflicts", Func: Resolve, ReqRepo: true, EventType: eventsapi.ClientEventType_CONF_RESOLVE},
})
