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

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var newShortDesc = "Create a new public/private keypair for authenticating with doltremoteapi."
var newLongDesc = `Creates a new keypair for authenticating with doltremoteapi.

Prints the public portion of the keypair, which can entered into the credentials
settings page of dolthub.`
var newSynopsis = []string{}

var newDocumentation = cli.CommandDocumentation{
	ShortDesc: newShortDesc,
	LongDesc: newLongDesc,
	Synopsis: newSynopsis,
}

type NewCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd NewCmd) Name() string {
	return "new"
}

// Description returns a description of the command
func (cmd NewCmd) Description() string {
	return newShortDesc
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd NewCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, commandStr, newDocumentation, ap)
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd NewCmd) RequiresRepo() bool {
	return false
}

// EventType returns the type of the event to log
func (cmd NewCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CREDS_NEW
}

func (cmd NewCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd NewCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, newDocumentation, ap)
	cli.ParseArgs(ap, args, help)

	_, newCreds, verr := actions.NewCredsFile(dEnv)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	err := updateConfigToUseNewCredIfNoExistingCred(dEnv, newCreds)
	if err != nil {
		verr = errhand.BuildDError("error: updating user.creds in dolt config to use new credentials").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	} else {
		return 0
	}
}

func updateConfigToUseNewCredIfNoExistingCred(dEnv *env.DoltEnv, dCreds creds.DoltCreds) error {
	gcfg, hasGCfg := dEnv.Config.GetConfig(env.GlobalConfig)

	if !hasGCfg {
		panic("global config not found.  Should create it here if this is a thing.")
	}

	_, err := gcfg.GetString(env.UserCreds)
	if err == config.ErrConfigParamNotFound {
		return gcfg.SetStrings(map[string]string{env.UserCreds: dCreds.KeyIDBase32Str()})
	} else {
		return err
	}
}
