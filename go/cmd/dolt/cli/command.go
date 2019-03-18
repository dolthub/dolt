package cli

import (
	"strings"

	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
)

// CommandFunc specifies the signature of the functions that will be called based on the command line being executed.
type CommandFunc func(string, []string, *env.DoltEnv) int

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

	return func(commandStr string, args []string, dEnv *env.DoltEnv) int {
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
				}
			}

			return command.Func(commandStr+" "+subCommandStr, args[1:], dEnv)
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
		Printf("    %16s - %s\n", command.Name, command.Desc)
	}
}
