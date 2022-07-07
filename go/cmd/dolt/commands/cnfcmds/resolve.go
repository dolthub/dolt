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

package cnfcmds

import (
	"context"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var resDocumentation = cli.CommandDocumentationContent{
	ShortDesc: "Automatically resolves all conflicts taking either ours or theirs for the given tables",
	LongDesc: `
	When a merge finds conflicting changes, it documents them in the dolt_conflicts table. A conflict is between two versions: ours (the rows at the destination branch head) and theirs (the rows at the source branch head).

	dolt conflicts resolve will automatically resolve the conflicts by taking either the ours or theirs versions for each row.
`,
	Synopsis: []string{
		`--ours|--theirs {{.LessThan}}table{{.GreaterThan}}...`,
	},
}

const (
	oursFlag   = "ours"
	theirsFlag = "theirs"
)

var autoResolveStrategies = map[string]AutoResolveStrategy{
	oursFlag:   AutoResolveStrategyOurs,
	theirsFlag: AutoResolveStrategyTheirs,
}

var autoResolverParams []string

func init() {
	autoResolverParams = make([]string, 0, len(autoResolveStrategies))
	for k := range autoResolveStrategies {
		autoResolverParams = append(autoResolverParams, k)
	}
}

type ResolveCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ResolveCmd) Name() string {
	return "resolve"
}

// Description returns a description of the command
func (cmd ResolveCmd) Description() string {
	return "Removes rows from list of conflicts"
}

func (cmd ResolveCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(resDocumentation, ap)
}

// EventType returns the type of the event to log
func (cmd ResolveCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CONF_RESOLVE
}

func (cmd ResolveCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "List of tables to be resolved. '.' can be used to resolve all tables."})
	ap.SupportsFlag("ours", "", "For all conflicts, take the version from our branch and resolve the conflict")
	ap.SupportsFlag("theirs", "", "For all conflicts, take the version from their branch and resolve the conflict")
	return ap
}

// Exec executes the command
func (cmd ResolveCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, resDocumentation, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if dEnv.IsLocked() {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), usage)
	}

	var verr errhand.VerboseError
	if apr.ContainsAny(autoResolverParams...) {
		verr = autoResolve(ctx, apr, dEnv)
	} else {
		verr = errhand.BuildDError("--ours or --theirs must be supplied").SetPrintUsage().Build()
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func autoResolve(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	funcFlags := apr.FlagsEqualTo(autoResolverParams, true)

	if funcFlags.Size() > 1 {
		ff := strings.Join(autoResolverParams, ", ")
		return errhand.BuildDError("specify only one from [ %s ]", ff).SetPrintUsage().Build()
	} else if apr.NArg() == 0 {
		return errhand.BuildDError("specify at least one table to resolve conflicts").SetPrintUsage().Build()
	}

	autoResolveFlag := funcFlags.AsSlice()[0]
	autoResolveStrategy := autoResolveStrategies[autoResolveFlag]

	var err error
	tbls := apr.Args
	if len(tbls) == 1 && tbls[0] == "." {
		err = AutoResolveAll(ctx, dEnv, autoResolveStrategy)
	} else {
		err = AutoResolveTables(ctx, dEnv, autoResolveStrategy, tbls)
	}

	if err != nil {
		return errhand.BuildDError("error: failed to resolve").AddCause(err).Build()
	}

	return saveDocsOnResolve(ctx, dEnv)
}

func saveDocsOnResolve(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	err := actions.SaveTrackedDocsFromWorking(ctx, dEnv)
	if err != nil {
		return errhand.BuildDError("error: failed to update docs on the filesystem").AddCause(err).Build()
	}
	return nil
}
