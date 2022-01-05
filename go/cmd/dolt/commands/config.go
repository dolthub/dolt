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

package commands

import (
	"context"
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

const (
	globalParamName   = "global"
	localParamName    = "local"
	addOperationStr   = "add"
	listOperationStr  = "list"
	getOperationStr   = "get"
	unsetOperationStr = "unset"
)

var cfgDocs = cli.CommandDocumentationContent{
	ShortDesc: `Get and set repository or global options`,
	LongDesc: `You can query/set/replace/unset options with this command.
		
	When reading, the values are read from the global and repository local configuration files, and options {{.LessThan}}--global{{.GreaterThan}}, and {{.LessThan}}--local{{.GreaterThan}} can be used to tell the command to read from only that location.
	
	When writing, the new value is written to the repository local configuration file by default, and options {{.LessThan}}--global{{.GreaterThan}}, can be used to tell the command to write to that location (you can say {{.LessThan}}--local{{.GreaterThan}} but that is the default).
`,

	Synopsis: []string{
		`[--global|--local] --list`,
		`[--global|--local] --add {{.LessThan}}name{{.GreaterThan}} {{.LessThan}}value{{.GreaterThan}}`,
		`[--global|--local] --get {{.LessThan}}name{{.GreaterThan}}`,
		`[--global|--local] --unset {{.LessThan}}name{{.GreaterThan}}...`,
	},
}

type ConfigCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ConfigCmd) Name() string {
	return "config"
}

// Description returns a description of the command
func (cmd ConfigCmd) Description() string {
	return "Dolt configuration."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd ConfigCmd) RequiresRepo() bool {
	return false
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ConfigCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, cfgDocs, ap))
}

func (cmd ConfigCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(globalParamName, "", "Use global config.")
	ap.SupportsFlag(localParamName, "", "Use repository local config.")
	ap.SupportsFlag(addOperationStr, "", "Set the value of one or more config parameters")
	ap.SupportsFlag(listOperationStr, "", "List the values of all config parameters.")
	ap.SupportsFlag(getOperationStr, "", "Get the value of one or more config parameters.")
	ap.SupportsFlag(unsetOperationStr, "", "Unset the value of one or more config parameters.")
	return ap
}

// Exec is used by the config command to allow users to view / edit their global and repository local configurations.
// Exec executes the command
func (cmd ConfigCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cfgDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	cfgTypes := apr.FlagsEqualTo([]string{globalParamName, localParamName}, true)
	ops := apr.FlagsEqualTo([]string{addOperationStr, listOperationStr, getOperationStr, unsetOperationStr}, true)

	if cfgTypes.Size() > 1 {
		cli.PrintErrln(color.RedString("Specifying both -local and -global is not valid. Exactly one may be set"))
		usage()
	} else {
		switch ops.Size() {
		case 1:
			return processConfigCommand(dEnv, cfgTypes, ops.AsSlice()[0], apr.Args, usage)
		default:
			cli.PrintErrln(color.RedString("Exactly one of the -add, -get, -unset, -list flags must be set."))
			usage()
		}
	}

	return 1
}

func processConfigCommand(dEnv *env.DoltEnv, setCfgTypes *set.StrSet, opName string, args []string, usage cli.UsagePrinter) int {
	switch opName {
	case getOperationStr:
		return getOperation(dEnv, setCfgTypes, args, func(k string, v *string) {
			cli.Println(*v)
		})
	case addOperationStr:
		return addOperation(dEnv, setCfgTypes, args, usage)
	case unsetOperationStr:
		return unsetOperation(dEnv, setCfgTypes, args, usage)
	case listOperationStr:
		return listOperation(dEnv, setCfgTypes, args, usage, func(k string, v string) {
			cli.Println(k, "=", v)
		})
	}

	panic("New operation added but not implemented.")
}

// Gets the config value for the key requested in the args, running the printFn given with the key and fetched value as
// arguments. If the key is not found, or if there is an error retrieving it, returns 1. Otherwise returns 0.
func getOperation(dEnv *env.DoltEnv, setCfgTypes *set.StrSet, args []string, printFn func(string, *string)) int {
	if len(args) != 1 {
		// matches git behavior... kinda dumb
		return 1
	}

	var cfg config.ReadableConfig
	switch setCfgTypes.Size() {
	case 0:
		cfg = dEnv.Config
	case 1:
		configElement := newCfgElement(setCfgTypes.AsSlice()[0])
		var ok bool
		cfg, ok = dEnv.Config.GetConfig(configElement)
		if !ok {
			cli.Println(color.RedString("No config found for %s", configElement.String()))
			return 1
		}
	default:
		// should be impossible due to earlier checks
		cli.Println(color.RedString("Cannot get more than one config scope at once"))
		return 1
	}

	val, err := cfg.GetString(args[0])
	if err != nil {
		if err != config.ErrConfigParamNotFound {
			cli.PrintErrln(color.RedString("Unexpected error: %s", err.Error()))
		}
		// Not found prints no error but returns status 1
		return 1
	}

	printFn(args[0], &val)
	return 0
}

func addOperation(dEnv *env.DoltEnv, setCfgTypes *set.StrSet, args []string, usage cli.UsagePrinter) int {
	if len(args) == 0 || len(args)%2 != 0 {
		cli.Println("error: wrong number of arguments")
		usage()
		return 1
	}

	updates := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		updates[strings.ToLower(args[i])] = args[i+1]
	}

	var cfgType string
	switch setCfgTypes.Size() {
	case 0:
		cfgType = localParamName
	case 1:
		cfgType = setCfgTypes.AsSlice()[0]
	default:
		cli.Println("error: cannot add to multiple configs simultaneously")
		return 1
	}

	cfg, ok := dEnv.Config.GetConfig(newCfgElement(cfgType))
	if !ok {
		switch cfgType {
		case globalParamName:
			panic("Should not have been able to get this far without a global config.")
		case localParamName:
			err := dEnv.Config.CreateLocalConfig(updates)
			if err != nil {
				cli.PrintErrln(color.RedString("Unable to create repo local config file"))
				return 1
			}
			return 0
		default:
			cli.Println("error: unknown config flag")
			return 1
		}
	}

	err := cfg.SetStrings(updates)
	if err != nil {
		cli.PrintErrln(color.RedString("Failed to update config."))
		return 1
	}

	cli.Println(color.CyanString("Config successfully updated."))
	return 0
}

func unsetOperation(dEnv *env.DoltEnv, setCfgTypes *set.StrSet, args []string, usage cli.UsagePrinter) int {
	if len(args) == 0 {
		cli.Println("error: wrong number of arguments")
		usage()
		return 1
	}

	for i, a := range args {
		args[i] = strings.ToLower(a)
	}

	var cfgType string
	switch setCfgTypes.Size() {
	case 0:
		cfgType = localParamName
	case 1:
		cfgType = setCfgTypes.AsSlice()[0]
	default:
		cli.Println("error: cannot unset from multiple configs simultaneously")
		return 1
	}

	if cfg, ok := dEnv.Config.GetConfig(newCfgElement(cfgType)); !ok {
		cli.PrintErrln(color.RedString("Unable to read config."))
		return 1
	} else {
		err := cfg.Unset(args)

		if err != nil && err != config.ErrConfigParamNotFound {
			cli.PrintErrln(color.RedString("Error unsetting the keys %v. Error: %s", args, err.Error()))
			return 1
		}

		cli.Println(color.CyanString("Config successfully updated."))
		return 0
	}
}

func listOperation(dEnv *env.DoltEnv, setCfgTypes *set.StrSet, args []string, usage cli.UsagePrinter, printFn func(string, string)) int {
	if len(args) != 0 {
		cli.Println("error: wrong number of arguments")
		usage()
		return 1
	}

	cfgTypesSl := setCfgTypes.AsSlice()
	for _, cfgType := range cfgTypesSl {
		if _, ok := dEnv.Config.GetConfig(newCfgElement(cfgType)); !ok {
			cli.PrintErrln(color.RedString("Unable to read config."))
			return 1
		}
	}

	if setCfgTypes.Size() == 0 {
		cfgTypesSl = []string{localParamName, globalParamName}
	}

	for _, cfgType := range cfgTypesSl {
		if cfg, ok := dEnv.Config.GetConfig(newCfgElement(cfgType)); ok {
			cfg.Iter(func(name, val string) bool {
				printFn(name, val)
				return false
			})
		}
	}

	return 0
}

func newCfgElement(configFlag string) env.ConfigScope {
	switch configFlag {
	case localParamName:
		return env.LocalConfig
	case globalParamName:
		return env.GlobalConfig
	default:
		return env.LocalConfig
	}
}
