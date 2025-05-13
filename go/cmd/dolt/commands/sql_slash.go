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
	ShowCmd{},
	AddCmd{},
	CommitCmd{},
	CheckoutCmd{},
	ResetCmd{},
	BranchCmd{},
	MergeCmd{},
	SlashHelp{},
	SlashEdit{},
	SlashPager{},
	WarningOn{},
	WarningOff{},
}

// parseSlashCmd parses a command line string into a slice of strings, splitting on spaces, but allowing spaces within
// double quotes. For example, the string `foo "bar baz"` would be parsed into the slice `[]string{"foo", "bar baz"}`.
// This is quick and dirty for slash command prototype, and doesn't try and handle all the crazy edge cases that come
// up with supporting many types of quotes. Also, pretty sure a dangling quote will break it. But it's a start.
func parseSlashCmd(cmd string) []string {
	cmd = strings.TrimPrefix(cmd, `\`)
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

// handleSlashCommand executes the command given by the fullCmd string. These are commands are direct calls to CLI commands.
func handleSlashCommand(sqlCtx *sql.Context, subCmd cli.Command, fullCmd string, cliCtx cli.CliContext) error {
	cliCmd := parseSlashCmd(fullCmd)
	if len(cliCmd) == 0 {
		return fmt.Errorf("Empty command. Use `\\help` for help.")
	}

	subCmdArgs := cliCmd[1:]
	status := subCmd.Exec(sqlCtx, subCmd.Name(), subCmdArgs, nil, cliCtx)
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
		CommandStr: "\\help",
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
	cli.Printf("Dolt CLI commands can be invoked with a leading '\\'. (e.g. '%s\\status')\n", prompt)
	cli.Println("\nAvailable commands:")
	for _, cmdInst := range slashCmds {
		cli.Println(fmt.Sprintf("  %10s - %s", cmdInst.Name(), cmdInst.Description()))
	}
	cli.Printf("\nFor more information on a specific command, type '\\help <command>' (e.g. '%s\\help status')\n", prompt)

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

// findSlashCmd finds a command by name in the list of slash commands. This function is meant to be flexible and can
// take just command names or a command with arguments and a "\" prefix.
func findSlashCmd(cmd string) (cli.Command, bool) {
	cmd = strings.TrimPrefix(cmd, `\`)
	words := strings.Split(cmd, " ")
	if len(words) == 0 {
		return nil, false
	}
	cmd = words[0]

	for _, cmdInst := range slashCmds {
		if cmdInst.Name() == cmd {
			return cmdInst, true
		}
	}
	return nil, false
}

type SlashEdit struct{}

var _ cli.Command = SlashEdit{}

func (s SlashEdit) Name() string {
	return "edit"
}

func (s SlashEdit) Description() string {
	return "Use $EDITOR to edit the last command."
}
func (s SlashEdit) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {

	initialCmd := "select * from my_table;"

	contents, err := execEditor(initialCmd, ".sql", cliCtx)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	cli.Printf("Edited command: %s", contents)

	return 0
}

func (s SlashEdit) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		ShortDesc: "Use $EDITOR to edit the last command.",
		LongDesc:  "",
		Synopsis:  []string{},
		ArgParser: s.ArgParser(),
	}
}

func (s SlashEdit) ArgParser() *argparser.ArgParser {
	// No arguments.
	return &argparser.ArgParser{}
}

type SlashPager struct{}

func (s SlashPager) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		ShortDesc: "Enable or Disable the result pager",
		LongDesc:  "Returns results in pager form. Use pager [on|off].",
		Synopsis:  []string{},
		ArgParser: s.ArgParser(),
	}
}

func (s SlashPager) ArgParser() *argparser.ArgParser {
	return &argparser.ArgParser{}
}

var _ cli.Command = SlashPager{}

func (s SlashPager) Name() string {
	return "pager"
}
func (s SlashPager) Description() string {
	return "Enable or Disable the result pager"
}

// Exec is a little special because the shell is interested in the response. So rather than call Exec, it calls
// handlePagerCommand function.
func (s SlashPager) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	panic("runtime error. SlashPager.Exec should never be called.")
}

// handlePagerCommand executes the pager command and returns true if pager should be on, or false otherwise. An error
// could come up if they provided weird input.
func handlePagerCommand(fullCmd string) (bool, error) {
	tokens := strings.Split(fullCmd, " ")

	if len(tokens) == 0 || tokens[0] != "\\pager" {
		return false, fmt.Errorf("runtime error: Expected \\pager command.")
	}

	if len(tokens) == 1 {
		return false, fmt.Errorf("Usage: \\pager [on|off]")
	}

	// Kind of sloppy here,`\pager foo bar on` will work, but not the end of the world.
	if tokens[len(tokens)-1] == "on" {
		return true, nil
	}
	if tokens[len(tokens)-1] == "off" {
		return false, nil
	}

	return false, fmt.Errorf("Usage: \\pager [on|off]")
}

type WarningCmd struct{}

func (s WarningCmd) Docs() *cli.CommandDocumentation {
	return &cli.CommandDocumentation{
		ShortDesc: "Toggle display of generated warnings after sql command.",
		LongDesc:  "Displays a detailed list of the warnings generated after each sql command. Use \\W and \\w to enable and disable the setting, respectively.",
		Synopsis:  []string{},
		ArgParser: s.ArgParser(),
	}
}

func (s WarningCmd) ArgParser() *argparser.ArgParser {
	return &argparser.ArgParser{}
}

// Exec should never be called on warning command; It only changes which information is displayed.
// handleWarningCommand should be used instead
func (s WarningCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	panic("runtime error. Exec should never be called on warning display commands.")
}

type WarningOn struct {
	WarningCmd
}

var _ cli.Command = WarningOn{}

func (s WarningOn) Name() string { return "W" }

func (s WarningOn) Description() string {
	return "Show generated warnings after sql command"
}

type WarningOff struct {
	WarningCmd
}

var _ cli.Command = WarningOff{}

func (s WarningOff) Name() string { return "w" }

func (s WarningOff) Description() string {
	return "Hide generated warnings after sql command"
}

func handleWarningCommand(fullCmd string) (bool, error) {
	tokens := strings.Split(fullCmd, " ")

	if len(tokens) == 0 || (tokens[0] != "\\w" && tokens[0] != "\\W") {
		return false, fmt.Errorf("runtime error: Expected \\w or \\W command.")
	}

	//Copied from mysql, could also return an error if more argument are passed in?
	if tokens[0] == "\\W" {
		return true, nil
	} else {
		return false, nil
	}
}
