// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type TransferCmd struct{}

func (cmd TransferCmd) Name() string {
	return "transfer"
}

func (cmd TransferCmd) Description() string {
	return "Transfer data to/from remote over stdin/stdout"
}

func (cmd TransferCmd) RequiresRepo() bool {
	return false
}

func (cmd TransferCmd) Hidden() bool {
	return true
}

func (cmd TransferCmd) InstallsSignalHandlers() bool {
	return true
}

var transferDocs = cli.CommandDocumentationContent{
	ShortDesc: "Internal command for SSH remote operations",
	LongDesc: `The transfer command is used internally by Dolt for SSH remote operations.
It serves repository data over stdin/stdout using multiplexed gRPC and HTTP protocols.

This command is typically invoked by SSH when cloning or pushing to SSH remotes:
  ssh user@host "dolt transfer /path/to/repo"

This is a low-level command not intended for direct use.`,
	Synopsis: []string{
		"<path>",
	},
}

func (cmd TransferCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(transferDocs, ap)
}

func (cmd TransferCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	return ap
}

func (cmd TransferCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, transferDocs, ap))
	_ = cli.ParseArgsOrDie(ap, args, help)

	// TODO: implement transfer command (serves gRPC + HTTP over stdio via SMUX)
	return HandleVErrAndExitCode(errhand.BuildDError("transfer command not yet implemented").Build(), nil)
}
