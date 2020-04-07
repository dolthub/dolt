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
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
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
		if isHelp(arg) {
			return true
		}
	}
	return false
}

// Command is the interface which defines a Dolt cli command
type Command interface {
	// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
	Name() string
	// Description returns a description of the command
	Description() string
	// Exec executes the command
	Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int
	// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
	CreateMarkdown(fs filesys.Filesys, path, commandStr string) error
}

// This type is to store the content of a documented command, elsewhere we can transform this struct into
// other structs that are used to generate documentation at the command line and in markdown files.
type CommandDocumentationContent struct {
	ShortDesc string
	LongDesc  string
	Synopsis  []string
}

//type CommandDocumentation

// RepoNotRequiredCommand is an optional interface that commands can implement if the command can be run without
// the current directory being a valid Dolt data repository.  Any commands not implementing this interface are
// assumed to require that they be run from a directory containing a Dolt data repository.
type RepoNotRequiredCommand interface {
	// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
	// that it be run from within a data repository directory
	RequiresRepo() bool
}

// EventMonitoredCommand is an optional interface that can be overridden in order to generate an event which is sent
// to the metrics system when the command is run
type EventMonitoredCommand interface {
	// EventType returns the type of the event to log
	EventType() eventsapi.ClientEventType
}

// HiddenCommand is an optional interface that can be overridden so that a command is hidden from the help text
type HiddenCommand interface {
	// Hidden should return true if this command should be hidden from the help text
	Hidden() bool
}

// SubCommandHandler is a command implementation which holds subcommands which can be called
type SubCommandHandler struct {
	name        string
	description string
	Subcommands []Command
}

// NewSubCommandHandler returns a new SubCommandHandler instance
func NewSubCommandHandler(name, description string, subcommands []Command) SubCommandHandler {
	return SubCommandHandler{name, description, subcommands}
}

func (hc SubCommandHandler) Name() string {
	return hc.name
}

func (hc SubCommandHandler) Description() string {
	return hc.description
}

func (hc SubCommandHandler) RequiresRepo() bool {
	return false
}

func (hc SubCommandHandler) CreateMarkdown(_ filesys.Filesys, _, _ string) error {
	return nil
}

func (hc SubCommandHandler) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	if len(args) < 1 {
		hc.printUsage(commandStr)
		return 1
	}

	subCommandStr := strings.ToLower(strings.TrimSpace(args[0]))
	for _, cmd := range hc.Subcommands {
		lwrName := strings.ToLower(cmd.Name())

		if lwrName == subCommandStr {
			cmdRequiresRepo := true
			if rnrCmd, ok := cmd.(RepoNotRequiredCommand); ok {
				cmdRequiresRepo = rnrCmd.RequiresRepo()
			}

			if cmdRequiresRepo && !hasHelpFlag(args) {
				isValid := CheckEnvIsValid(dEnv)
				if !isValid {
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

func CheckEnvIsValid(dEnv *env.DoltEnv) bool {
	if !dEnv.HasDoltDir() {
		PrintErrln(color.RedString("The current directory is not a valid dolt repository."))
		PrintErrln("run: dolt init before trying to run this command")
		return false
	} else if dEnv.RSLoadErr != nil {
		PrintErrln(color.RedString("The current directories repository state is invalid"))
		PrintErrln(dEnv.RSLoadErr.Error())
		return false
	} else if dEnv.DBLoadError != nil {
		PrintErrln(color.RedString("Failed to load database."))
		PrintErrln(dEnv.DBLoadError.Error())
		return false
	}

	return true
}

func (hc SubCommandHandler) printUsage(commandStr string) {
	Println("Valid commands for", commandStr, "are")

	for _, cmd := range hc.Subcommands {
		if hiddenCmd, ok := cmd.(HiddenCommand); ok {
			if hiddenCmd.Hidden() {
				continue
			}
		}

		Printf("    %16s - %s\n", cmd.Name(), cmd.Description())
	}
}
