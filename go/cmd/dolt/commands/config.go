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

package commands

import (
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

const (
	globalParamName = "global"
	localParamName  = "local"

	addOperationStr   = "add"
	listOperationStr  = "list"
	getOperationStr   = "get"
	unsetOperationStr = "unset"
)

var cfgShortDesc = `Get and set repository or global options`
var cfgLongDesc = `You can query/set/replace/unset options with this command.

When reading, the values are read from the global and repository local configuration files, and options --global, and --local can be used to tell the command to read from only that location.

When writing, the new value is written to the repository local configuration file by default, and options --global, can be used to tell the command to write to that location (you can say --local but that is the default).`
var cfgSynopsis = []string{
	"[--global|--local] --list",
	"[--global|--local] --add <name> <value>",
	"[--global|--local] --get <name>",
	"[--global|--local] --unset <name>...",
}

// Config is used by the config command to allow users to view / edit their global and repository local configurations.
func Config(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(globalParamName, "", "Use global config.")
	ap.SupportsFlag(localParamName, "", "Use repository local config.")
	ap.SupportsFlag(addOperationStr, "", "Set the value of one or more config parameters")
	ap.SupportsFlag(listOperationStr, "", "List the values of all config parameters.")
	ap.SupportsFlag(getOperationStr, "", "Get the value of one or more config parameters.")
	ap.SupportsFlag(unsetOperationStr, "", "Unset the value of one or more config paramaters.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, cfgShortDesc, cfgLongDesc, cfgSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	cfgTypes := apr.FlagsEqualTo([]string{globalParamName, localParamName}, true)
	ops := apr.FlagsEqualTo([]string{addOperationStr, listOperationStr, getOperationStr, unsetOperationStr}, true)

	if cfgTypes.Size() == 2 {
		cli.PrintErrln(color.RedString("Specifying both -local and -global is not valid. Exactly one may be set"))
		usage()
	} else {
		switch ops.Size() {
		case 1:
			return processConfigCommand(dEnv, cfgTypes, ops.AsSlice()[0], apr.Args(), usage)
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

	cfgTypesSl := setCfgTypes.AsSlice()
	for _, cfgType := range cfgTypesSl {
		isGlobal := cfgType == globalParamName
		if _, ok := dEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
			cli.PrintErrln(color.RedString("Unable to read config."))
			return 1
		}
	}

	if setCfgTypes.Size() == 0 {
		cfgTypesSl = []string{localParamName, globalParamName}
	}

	for _, cfgType := range cfgTypesSl {
		isGlobal := cfgType == globalParamName
		cfg, ok := dEnv.Config.GetConfig(newCfgElement(isGlobal))
		if ok {
			if val, err := cfg.GetString(args[0]); err == nil {
				printFn(args[0], &val)
				return 0
			} else if err != config.ErrConfigParamNotFound {
				cli.PrintErrln(color.RedString("Unexpected error: %s", err.Error()))
				return 1
			}
		}
	}

	return 1
}

func addOperation(dEnv *env.DoltEnv, setCfgTypes *set.StrSet, args []string, usage cli.UsagePrinter) int {
	if len(args)%2 != 0 {
		cli.Println("error: wrong number of arguments")
		usage()
		return 1
	}

	isGlobal := setCfgTypes.Contains(globalParamName)
	updates := make(map[string]string)

	for i := 0; i < len(args); i += 2 {
		updates[strings.ToLower(args[i])] = args[i+1]
	}

	if cfg, ok := dEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
		if !isGlobal {
			err := dEnv.Config.CreateLocalConfig(updates)

			if err != nil {
				cli.PrintErrln(color.RedString("Unable to create repo local config file"))
				return 1
			}

		} else {
			panic("Should not have been able to get this far without a global config.")
		}
	} else {
		err := cfg.SetStrings(updates)

		if err != nil {
			cli.PrintErrln(color.RedString("Failed to update config."))
			return 1
		}
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

	isGlobal := setCfgTypes.Contains(globalParamName)
	if cfg, ok := dEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
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
		isGlobal := cfgType == globalParamName
		if _, ok := dEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
			cli.PrintErrln(color.RedString("Unable to read config."))
			return 1
		}
	}

	if setCfgTypes.Size() == 0 {
		cfgTypesSl = []string{localParamName, globalParamName}
	}

	for _, cfgType := range cfgTypesSl {
		isGlobal := cfgType == globalParamName
		cfg, ok := dEnv.Config.GetConfig(newCfgElement(isGlobal))
		if ok {
			cfg.Iter(func(name string, val string) (stop bool) {
				printFn(name, val)

				return false
			})
		}
	}

	return 0
}

func newCfgElement(isGlobal bool) env.DoltConfigElement {
	if isGlobal {
		return env.GlobalConfig
	}

	return env.LocalConfig
}
