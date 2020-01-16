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

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var rmShortDesc = "Remove a stored public/private keypair."
var rmLongDesc = `Removes an existing keypair from dolt's credential storage.`
var rmSynopsis = []string{"<public_key_as_appears_in_ls>"}

type RmCmd struct{}

func (cmd RmCmd) Name() string {
	return "rm"
}

func (cmd RmCmd) Description() string {
	return rmShortDesc
}

func (cmd RmCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, rmShortDesc, rmLongDesc, rmSynopsis, ap)
}

func (cmd RmCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

func (cmd RmCmd) RequiresRepo() bool {
	return false
}

func (cmd RmCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_RM
}

func (cmd RmCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, rmShortDesc, rmLongDesc, rmSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	credsDir, verr := actions.EnsureCredsDir(dEnv)

	if verr == nil {
		for _, arg := range args {
			jwkFilePath, err := dEnv.FindCreds(credsDir, arg)

			if err == nil {
				err = dEnv.FS.DeleteFile(jwkFilePath)
			}

			if err != nil {
				cli.Println(color.YellowString("failed to delete %s: %s", arg, err.Error()))
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
