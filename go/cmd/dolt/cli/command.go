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

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
)

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

type Command interface {
	Name() string
	Description() string
	Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int
}

type RepoNotRequiredCommand interface {
	RequiresRepo() bool
}

type EventMonitoredCommand interface {
	EventType() eventsapi.ClientEventType
}

type HiddenCommand interface {
	Hidden() bool
}

type HandlerCommand struct {
	name        string
	description string
	subcommands []Command
}

func NewHandlerCommand(name, description string, subcommands []Command) HandlerCommand {
	return HandlerCommand{name, description, subcommands}
}

func (hc HandlerCommand) Name() string {
	return hc.name
}

func (hc HandlerCommand) Description() string {
	return hc.description
}

func (hc HandlerCommand) RequiresRepo() bool {
	return false
}

func (hc HandlerCommand) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	if len(args) < 1 {
		hc.printUsage(commandStr)
		return 1
	}

	subCommandStr := strings.ToLower(strings.TrimSpace(args[0]))
	for _, cmd := range hc.subcommands {
		lwrName := strings.ToLower(cmd.Name())

		if lwrName == subCommandStr {
			cmdRequiresRepo := true
			if rnrCmd, ok := cmd.(RepoNotRequiredCommand); ok {
				cmdRequiresRepo = rnrCmd.RequiresRepo()
			}

			if cmdRequiresRepo && !hasHelpFlag(args) {
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
			if evtCmd, ok := cmd.(EventMonitoredCommand); ok {
				evt = events.NewEvent(evtCmd.EventType())
				ctx = events.NewContextForEvent(ctx, evt)
			}

			ret := cmd.Exec(ctx, commandStr+" "+subCommandStr, args[1:], dEnv)

			if evt != nil {
				events.GlobalCollector.CloseEventAndAdd(evt)
			}

			return ret
		}
	}

	if !isHelp(subCommandStr) {
		PrintErrln(color.RedString("Unknown Command " + subCommandStr))
	}

	hc.printUsage(commandStr)
	return 1
}

func (hc HandlerCommand) printUsage(commandStr string) {
	Println("Valid commands for", commandStr, "are")

	for _, cmd := range hc.subcommands {
		if hiddenCmd, ok := cmd.(HiddenCommand); ok {
			if hiddenCmd.Hidden() {
				continue
			}
		}

		Printf("    %16s - %s\n", cmd.Name(), cmd.Description())
	}
}
