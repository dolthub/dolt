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

package cli

import (
	"context"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fatih/color"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/nbs"
)

func isHelp(str string) bool {
	str = strings.TrimSpace(str)

	if len(str) == 0 {
		return false
	}

	if str[0] != '-' {
		return false
	}

	str = strings.ToLower(strings.TrimLeft(str, "- "))

	return str == "h" || str == "help"
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
	// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
	Name() string
	// Description returns a description of the command
	Description() string
	// Exec executes the command
	Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int
	// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
	CreateMarkdown(writer io.Writer, commandStr string) error
	// ArgParser returns the arg parser for this command
	ArgParser() *argparser.ArgParser
}

// SignalCommand is an extension of Command that allows commands to install their own signal handlers, rather than use
// the global one (which cancels the global context).
type SignalCommand interface {
	Command

	// InstallsSignalHandlers returns whether this command manages its own signal handlers for interruption / termination.
	InstallsSignalHandlers() bool
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
	// Unspecified ONLY applies when no other command has been given. This is different from how a default command would
	// function, as a command that doesn't exist for this sub handler will result in an error.
	Unspecified Command
	Subcommands []Command
	hidden      bool
}

// NewSubCommandHandler returns a new SubCommandHandler instance
func NewSubCommandHandler(name, description string, subcommands []Command) SubCommandHandler {
	return SubCommandHandler{name, description, nil, subcommands, false}
}

// NewHiddenSubCommandHandler returns a new SubCommandHandler instance that is hidden from display
func NewHiddenSubCommandHandler(name, description string, subcommands []Command) SubCommandHandler {
	return SubCommandHandler{name, description, nil, subcommands, true}
}

// NewSubCommandHandlerWithUnspecified returns a new SubCommandHandler that will invoke the unspecified command ONLY if
// no direct command is given.
func NewSubCommandHandlerWithUnspecified(name, description string, hidden bool, unspecified Command, subcommands []Command) SubCommandHandler {
	return SubCommandHandler{name, description, unspecified, subcommands, hidden}
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

func (hc SubCommandHandler) CreateMarkdown(_ io.Writer, _ string) error {
	return nil
}

func (hc SubCommandHandler) ArgParser() *argparser.ArgParser {
	return nil
}

func (hc SubCommandHandler) Hidden() bool {
	return hc.hidden
}

func (hc SubCommandHandler) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	if len(args) < 1 && hc.Unspecified == nil {
		hc.printUsage(commandStr)
		return 1
	}

	var subCommandStr string
	if len(args) > 0 {
		subCommandStr = strings.ToLower(strings.TrimSpace(args[0]))
	}

	for _, cmd := range hc.Subcommands {
		lwrName := strings.ToLower(cmd.Name())
		if lwrName == subCommandStr {
			return hc.handleCommand(ctx, commandStr+" "+subCommandStr, cmd, args[1:], dEnv)
		}
	}
	if hc.Unspecified != nil {
		return hc.handleCommand(ctx, commandStr, hc.Unspecified, args, dEnv)
	}

	if !isHelp(subCommandStr) {
		PrintErrln(color.RedString("Unknown Command " + subCommandStr))
		return 1
	}

	hc.printUsage(commandStr)
	return 0
}

func (hc SubCommandHandler) handleCommand(ctx context.Context, commandStr string, cmd Command, args []string, dEnv *env.DoltEnv) int {
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

	// Certain commands cannot tolerate a top-level signal handler (which cancels the root context) but need to manage
	// their own interrupt semantics.
	if signalCmd, ok := cmd.(SignalCommand); !ok || !signalCmd.InstallsSignalHandlers() {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
	}

	ret := cmd.Exec(ctx, commandStr, args, dEnv)

	if evt != nil {
		events.GlobalCollector.CloseEventAndAdd(evt)
	}

	return ret
}

// CheckEnvIsValid validates that a DoltEnv has been initialized properly and no errors occur during load, and prints
// error messages to the user if there are issues with the environment or if errors were encountered while loading it.
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
		if dEnv.DBLoadError == nbs.ErrUnreadableManifest {
			PrintErrln("\tyou might need to upgrade your Dolt client")
			PrintErrln("\tvisit https://github.com/dolthub/dolt/releases/latest/")
		}
		return false
	}

	return true
}

const (
	userNameRequiredError = `Author identity unknown

*** Please tell me who you are.

Run

  dolt config --global --add user.email "you@example.com"
  dolt config --global --add user.name "Your Name"

to set your account's default identity.
Omit --global to set the identity only in this repository.

fatal: empty ident name not allowed
`
)

// CheckUserNameAndEmail returns true if the user name and email are set for this environment, or prints an error and
// returns false if not.
func CheckUserNameAndEmail(dEnv *env.DoltEnv) bool {
	_, err := dEnv.Config.GetString(env.UserEmailKey)
	if err != nil {
		PrintErr(userNameRequiredError)
		return false
	}

	_, err = dEnv.Config.GetString(env.UserNameKey)
	if err != nil {
		PrintErr(userNameRequiredError)
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
