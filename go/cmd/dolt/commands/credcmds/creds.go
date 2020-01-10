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

package credcmds

import (
	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
)

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "new", Desc: newShortDesc, Func: New, ReqRepo: false, EventType: eventsapi.ClientEventType_CREDS_NEW},
	{Name: "rm", Desc: rmShortDesc, Func: Rm, ReqRepo: false, EventType: eventsapi.ClientEventType_CREDS_RM},
	{Name: "ls", Desc: lsShortDesc, Func: Ls, ReqRepo: false, EventType: eventsapi.ClientEventType_CREDS_LS},
	// TODO(aaron): Command to select a credential by public key and update global/repo config
	// to use it for authentication.
	//{Name: "use", Desc: useShortDesc, Func: Ls, ReqRepo: false, EventType: eventsapi.ClientEventType_CREDS_USE},
	{Name: "check", Desc: checkShortDesc, Func: Check, ReqRepo: false, EventType: eventsapi.ClientEventType_CREDS_CHECK},
})
