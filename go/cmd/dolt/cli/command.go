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

package cli

import (
	"context"
	"strings"

	"github.com/fatih/color"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
)

// CommandFunc specifies the signature of the functions that will be called based on the command line being executed.
type CommandFunc func(context.Context, string, []string, *env.DoltEnv) int

// Command represents either a command to be run, or a command that is a parent of a subcommand.
type Command struct {
	// Name is what the user will type on the command line in order to execute this command
	Name string
	// Desc is a short description of the command
	Desc string
	// Func is the CommandFunc that gets called when the user executes this command
	Func CommandFunc
	// ReqRepo says whether the command must be executed in an initialized dolt data repository directory.  This should
	// always be set to false for non leaf commands.
	ReqRepo bool
	// Hide says whether to hide this command from help listings (for features that aren't ready to be released publicly).
	HideFromHelp bool
	// A client event associated with this command
	EventType eventsapi.ClientEventType
}

// MapCommands takes a list of commands and maps them based on the commands name
func MapCommands(commands []*Command) map[string]*Command {
	commandMap := make(map[string]*Command, len(commands))

	for _, command := range commands {
		commandMap[strings.ToLower(command.Name)] = command
	}

	return commandMap
}

// GenSubCommandHandler returns a handler function that will handle subcommand processing.
func GenSubCommandHandler(commands []*Command) CommandFunc {
	commandMap := MapCommands(commands)

	return func(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
		if len(args) < 1 {
			printUsage(commandStr, commands)
			return 1
		}

		subCommandStr := strings.ToLower(strings.TrimSpace(args[0]))
		if command, ok := commandMap[subCommandStr]; ok {
			if command.ReqRepo && !hasHelpFlag(args) {
				if !dEnv.HasDoltDir() {
					PrintErrln(color.RedString("The current directory is not a valid dolt repository."))
					PrintErrln("run: dolt init before trying to run this command")
					return 2
				} else if dEnv.RSLoadErr != nil {
					PrintErrln(color.RedString("The current directories repository state is invalid"))
					PrintErrln(dEnv.RSLoadErr.Error())
					return 2
				} else if dEnv.DBLoadError != nil {
					PrintErrln(color.RedString("Failed to load database."))
					PrintErrln(dEnv.DBLoadError.Error())
					return 2
				}
			}

			var evt *events.Event
			if command.EventType != eventsapi.ClientEventType_TYPE_UNSPECIFIED {
				evt = events.NewEvent(command.EventType)
				ctx = events.NewContextForEvent(ctx, evt)
			}

			ret := command.Func(ctx, commandStr+" "+subCommandStr, args[1:], dEnv)

			if evt != nil {
				dEnv.SetStandardEventAttributes(evt)
				events.GlobalCollector.CloseEventAndAdd(evt)
			}

			return ret
		}

		if !isHelp(subCommandStr) {
			PrintErrln(color.RedString("Unknown Command " + subCommandStr))
		}
		printUsage(commandStr, commands)
		return 1
	}
}

func isHelp(str string) bool {
	switch {
	case str == "-h":
		return true
	case strings.TrimLeft(str, "- ") == "help":
		return true
	}

	return false
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return true
		}
	}
	return false
}

func printUsage(commandStr string, commands []*Command) {
	Println("Valid commands for", commandStr, "are")

	for _, command := range commands {
		if !command.HideFromHelp {
			Printf("    %16s - %s\n", command.Name, command.Desc)
		}
	}
}
