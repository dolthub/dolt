// Copyright 2024 Dolthub, Inc.
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
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var slashCmds = []cli.Command{
	StatusCmd{},
	DiffCmd{},
	LogCmd{},
	AddCmd{},
	CommitCmd{},
	CheckoutCmd{},
	ResetCmd{},
	BranchCmd{},
	MergeCmd{},
	SlashHelp{},
}

// parseSlashCmd parses a command line string into a slice of strings, splitting on spaces, but allowing spaces within
// double quotes. For example, the string `foo "bar baz"` would be parsed into the slice `[]string{"foo", "bar baz"}`.
// This is quick and dirty for slash command prototype, and doesn't try and handle all the crazy edge cases that come
// up with supporting many types of quotes. Also, pretty sure a dangling quote will break it. But it's a start.
func parseSlashCmd(cmd string) []string {

	// TODO: determine if we can get rid of the ";" as the terminator for cli commands.
	cmd = strings.TrimSuffix(cmd, ";")
	cmd = strings.TrimRight(cmd, " \t\n\r\v\f")
	cmd = strings.TrimLeft(cmd, " \t\n\r\v\f")

	r := regexp.MustCompile(`"[^"\\]*(?:\\.[^"\\]*)*"|\S+`)
	cmdWords := r.FindAllString(cmd, -1)

	for i := range cmdWords {
		if cmdWords[i][0] == '"' {
			cmdWords[i] = cmdWords[i][1 : len(cmdWords[i])-1]
			cmdWords[i] = strings.ReplaceAll(cmdWords[i], `\"`, `"`)
		}
	}

	if len(cmdWords) == 0 {
		return []string{}
	}

	return cmdWords
}

func handleSlashCommand(sqlCtx *sql.Context, fullCmd string, cliCtx cli.CliContext) error {
	cliCmd := parseSlashCmd(fullCmd)
	if len(cliCmd) == 0 {
		return fmt.Errorf("Empty command. Use `/help;` for help.")
	}

	subCmd := cliCmd[0]
	subCmdArgs := cliCmd[1:]
	status := 1

	subCmdInst, ok := findSlashCmd(subCmd)
	if ok {
		status = subCmdInst.Exec(sqlCtx, subCmd, subCmdArgs, nil, cliCtx)
	} else {
		return fmt.Errorf("Unknown command: %s. Use `/help;` for a list of command.", subCmd)
	}

	if status != 0 {
		return fmt.Errorf("error executing command: %s", cliCmd)
	}
	return nil
}

type SlashHelp struct{}

func (s SlashHelp) Name() string {
	return "help"
}

func (s SlashHelp) Description() string {
	return "What you see right now."
}

func (s SlashHelp) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		CommandStr: "/help",
		ShortDesc:  "What you see right now.",
		LongDesc:   "It would seem that you are crying out for help. Please join us on Discord (https://discord.gg/gqr7K4VNKe)!",
		Synopsis:   []string{},
		ArgParser:  s.ArgParser(),
	}
}

func (s SlashHelp) Exec(ctx context.Context, _ string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	if args != nil && len(args) > 0 {
		subCmd := args[0]
		subCmdInst, ok := findSlashCmd(subCmd)
		if ok {
			foo, _ := cli.HelpAndUsagePrinters(subCmdInst.Docs())
			foo()

		} else {
			cli.Println(fmt.Sprintf("Unknown command: %s", subCmd))
		}
		return 0
	}

	qryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if closeFunc != nil {
		defer closeFunc()
	}
	if err != nil {
		cli.Println(fmt.Sprintf("error getting query engine: %s", err))
		return 1
	}

	prompt := generateHelpPrompt(sqlCtx, qryist)

	cli.Println("Dolt SQL Shell Help")
	cli.Printf("Default behavior is to interpret SQL statements.     (e.g. '%sselect * from my_table;')\n", prompt)
	cli.Printf("Dolt CLI commands can be invoked with a leading '/'. (e.g. '%s/status;')\n", prompt)
	cli.Println("All statements are terminated with a ';'.")
	cli.Println("\nAvailable commands:")
	for _, cmdInst := range slashCmds {
		cli.Println(fmt.Sprintf("  %10s - %s", cmdInst.Name(), cmdInst.Description()))
	}
	cli.Printf("\nFor more information on a specific command, type '/help <command>;' (e.g. '%s/help status;')\n", prompt)

	moreWords := `
-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
Still need assistance? Talk directly to Dolt developers on Discord! https://discord.gg/gqr7K4VNKe
Found a bug? Want additional features? Please let us know! https://github.com/dolthub/dolt/issues
-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-`

	cli.Println(moreWords)

	return 0
}

func generateHelpPrompt(sqlCtx *sql.Context, qryist cli.Queryist) string {
	db, branch, _ := getDBBranchFromSession(sqlCtx, qryist)
	dirty := false
	if branch != "" {
		dirty, _ = isDirty(sqlCtx, qryist)
	}
	prompt, _ := formattedPrompts(db, branch, dirty)
	return prompt
}

func (s SlashHelp) ArgParser() *argparser.ArgParser {
	return &argparser.ArgParser{}
}

func findSlashCmd(cmd string) (cli.Command, bool) {
	for _, cmdInst := range slashCmds {
		if cmdInst.Name() == cmd {
			return cmdInst, true
		}
	}
	return nil, false
}
