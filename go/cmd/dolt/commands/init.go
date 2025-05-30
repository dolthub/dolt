// Copyright 2020 Dolthub, Inc.
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

package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	emailParamName      = "email"
	usernameParamName   = "name"
	initBranchParamName = "initial-branch"
	funHashFlag         = "fun"
)

var initDocs = cli.CommandDocumentationContent{
	ShortDesc: "Create an empty Dolt data repository",
	LongDesc: `This command creates an empty Dolt data repository in the current directory.

Running dolt init in an already initialized directory will fail.
`,

	Synopsis: []string{
		"",
	},
}

type InitCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd InitCmd) Name() string {
	return "init"
}

// Description returns a description of the command
func (cmd InitCmd) Description() string {
	return "Create an empty Dolt data repository."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd InitCmd) RequiresRepo() bool {
	return false
}

func (cmd InitCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(initDocs, ap)
}

func (cmd InitCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(usernameParamName, "", "name", fmt.Sprintf("The name used in commits to this repo. If not provided will be taken from {{.EmphasisLeft}}%s{{.EmphasisRight}} in the global config.", config.UserNameKey))
	ap.SupportsString(emailParamName, "", "email", fmt.Sprintf("The email address used. If not provided will be taken from {{.EmphasisLeft}}%s{{.EmphasisRight}} in the global config.", config.UserEmailKey))
	ap.SupportsString(cli.DateParam, "", "date", "Specify the date used in the initial commit. If not specified the current system time is used.")
	ap.SupportsString(initBranchParamName, "b", "branch", fmt.Sprintf("The branch name used to initialize this database. If not provided will be taken from {{.EmphasisLeft}}%s{{.EmphasisRight}} in the global config. If unset, the default initialized branch will be named '%s'.", config.InitBranchName, env.DefaultInitBranch))
	ap.SupportsFlag(funHashFlag, "", "") // This flag is an easter egg. We can't currently prevent it from being listed in the help, but the description is deliberately left blank.
	return ap
}

// Exec executes the command
func (cmd InitCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, initDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if len(apr.Args) > 0 {
		cli.PrintErrln(color.RedString("error: invalid arguments."))
		return 1
	}

	if dEnv.HasDoltDataDir() {
		cli.PrintErrln(color.RedString("This directory has already been initialized."))
		return 1
	}

	if dEnv.HasDoltSqlServerInfo() {
		cli.PrintErrln(color.RedString("Detected that a Dolt sql-server is running from this directory. " +
			"Stop the sql-server before initializing this directory as a Dolt database."))
		return 1
	}

	name, _ := apr.GetValue(usernameParamName)
	email, _ := apr.GetValue(emailParamName)
	initBranch, _ := apr.GetValue(initBranchParamName)

	if len(name) == 0 || len(email) == 0 {
		// This command creates a commit, so we need user identity
		if !cli.CheckUserNameAndEmail(cliCtx.Config()) {
			return 1
		}
	}

	name = dEnv.Config.IfEmptyUseConfig(name, config.UserNameKey)
	email = dEnv.Config.IfEmptyUseConfig(email, config.UserEmailKey)
	if initBranch == "" {
		initBranch = env.GetDefaultInitBranch(dEnv.Config)
	}

	if name == "" {
		cli.PrintErrln(
			color.RedString("Could not determine %[1]s. "+
				"Use the init parameter --name \"FIRST LAST\" to set it for this repo, "+
				"or dolt config --global --add %[1]s \"FIRST LAST\"", config.UserNameKey))
		usage()
		return 1
	} else if email == "" {
		cli.PrintErrln(
			color.RedString("Could not determine %[1]s. "+
				"Use the init parameter --email \"EMAIL_ADDRESS\" to set it for this repo, "+
				"or dolt config --global --add %[1]s \"EMAIL_ADDRESS\"", config.UserEmailKey))
		usage()
		return 1
	}

	t := time.Now()
	if commitTimeStr, ok := apr.GetValue(cli.DateParam); ok {
		var err error
		t, err = dconfig.ParseDate(commitTimeStr)

		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: invalid date").AddCause(err).Build(), usage)
		}
	}

	requiresFunHash := apr.Contains(funHashFlag)
	commitMetaGenerator := datas.MakeCommitMetaGenerator(name, email, t)
	if requiresFunHash {
		commitMetaGenerator = datas.MakeFunCommitMetaGenerator(name, email, t)
	}

	err := dEnv.InitRepoWithCommitMetaGenerator(context.Background(), types.Format_Default, initBranch, commitMetaGenerator)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to initialize directory as a data repo. %s", err.Error()))
		return 1
	}

	configuration := make(map[string]string)
	if apr.Contains(usernameParamName) {
		configuration[config.UserNameKey] = name
	}
	if apr.Contains(emailParamName) {
		configuration[config.UserEmailKey] = email
	}
	if len(configuration) > 0 {
		err = dEnv.Config.WriteableConfig().SetStrings(configuration)
		if err != nil {
			cli.PrintErrln(color.RedString("Failed to store initial configuration. %s", err.Error()))
			return 1
		}
	}

	cli.Println(color.CyanString("Successfully initialized dolt data repository."))
	return 0
}
