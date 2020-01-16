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
	"context"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var newShortDesc = "Create a new public/private keypair for authenticating with doltremoteapi."
var newLongDesc = `Creates a new keypair for authenticating with doltremoteapi.

Prints the public portion of the keypair, which can entered into the credentials
settings page of dolthub.`
var newSynopsis = []string{}

type NewCmd struct{}

func (cmd NewCmd) Name() string {
	return "new"
}

func (cmd NewCmd) Description() string {
	return newShortDesc
}

func (cmd NewCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, newShortDesc, newLongDesc, newSynopsis, ap)
}

func (cmd NewCmd) RequiresRepo() bool {
	return false
}

func (cmd NewCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_NEW
}

func (cmd NewCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

func (cmd NewCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, newShortDesc, newLongDesc, newSynopsis, ap)
	cli.ParseArgs(ap, args, help)

	_, _, verr := actions.NewCredsFile(dEnv)

	return commands.HandleVErrAndExitCode(verr, usage)
}
