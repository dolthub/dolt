// Copyright 2019 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

var newDocs = cli.CommandDocumentationContent{
	ShortDesc: "Create a new public/private keypair for authenticating with doltremoteapi.",
	LongDesc: `Creates a new keypair for authenticating with doltremoteapi.

Prints the public portion of the keypair, which can entered into the credentials settings page of dolthub.`,
	Synopsis: []string{},
}

type NewCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd NewCmd) Name() string {
	return "new"
}

// Description returns a description of the command
func (cmd NewCmd) Description() string {
	return newDocs.ShortDesc
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd NewCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return commands.CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, newDocs, ap))
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

func (cmd NewCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd NewCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, newDocs, ap))
	cli.ParseArgsOrDie(ap, args, help)

	_, newCreds, verr := actions.NewCredsFile(dEnv)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	cli.Println("Credentials created successfully.")
	cli.Println("pub key:", newCreds.PubKeyBase32Str())

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
