package commands

import (
	"errors"
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/funcitr"
	"os"
	"strings"
)

const (
	globalParamName = "global"
	localParamName  = "local"

	setOperationStr   = "set"
	listOperationStr  = "list"
	getOperationStr   = "get"
	unsetOperationStr = "unset"
)

func configUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

// Config is used by the config command to allow users to view / edit their global and repository local configurations.
func Config(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = configUsage(fs)

	cfgTypeFlagMap := cli.NewBoolFlagMap(fs, map[string]string{
		globalParamName: "Use global config file.",
		localParamName:  "Use repository config file.",
	})

	opFlagMap := cli.NewBoolFlagMap(fs, map[string]string{
		setOperationStr:   "Set the value of one or more config parameters",
		listOperationStr:  "List the values of all config parameters.",
		getOperationStr:   "Get the value of one or more config parameters.",
		unsetOperationStr: "Unset the value of one or more config paramaters.",
	})

	fs.Parse(args)

	cfgTypes := cfgTypeFlagMap.GetEqualTo(true)
	ops := opFlagMap.GetEqualTo(true)

	switch cfgTypes.Size() {
	case 2:
		fmt.Fprintln(os.Stderr, color.RedString("Specifying both -local and -global is not valid. Exactly one may be set"))
	case 0:
		fmt.Fprintln(os.Stderr, color.RedString("One of the -local or -global flags must be set"))
	case 1:
		switch ops.Size() {
		case 1:
			lwrArgs := funcitr.MapStrings(fs.Args(), strings.ToLower)
			return processConfigCommand(cliEnv, cfgTypes.AsSlice()[0] == globalParamName, ops.AsSlice()[0], lwrArgs)
		default:
			fmt.Fprintln(os.Stderr, color.RedString("Exactly one of the -set, -get, -unset, -list flags must be set."))
		}
	}

	return 1
}

func processConfigCommand(cliEnv *env.DoltCLIEnv, isGlobal bool, opName string, args []string) int {
	switch opName {
	case getOperationStr:
		return getOperation(cliEnv, isGlobal, args, func(k string, v *string) {
			if v == nil {
				fmt.Println(k, color.YellowString(" <NOT SET>"))
			} else {
				fmt.Println(k, "=", *v)
			}
		})
	case setOperationStr:
		return setOperation(cliEnv, isGlobal, args)
	case unsetOperationStr:
		return unsetOperation(cliEnv, isGlobal, args)
	case listOperationStr:
		return listOperation(cliEnv, isGlobal, func(k string, v string) {
			fmt.Println(k, "=", v)
		})
	}

	panic("New operation added but not implemented.")
}

func getOperation(cliEnv *env.DoltCLIEnv, isGlobal bool, args []string, printFn func(string, *string)) int {
	if cfg, ok := cliEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
		fmt.Fprintln(os.Stderr, color.RedString("Unable to read config."))
		return 1
	} else {
		for _, param := range args {
			if val, err := cfg.GetString(param); err == nil {
				printFn(param, &val)
			} else if err == config.ErrConfigParamNotFound {
				printFn(param, nil)
			} else {
				fmt.Fprintln(os.Stderr, color.RedString("Unexpected error: %s", err.Error()))
				return 1
			}

		}
		return 0
	}
}

func setOperation(cliEnv *env.DoltCLIEnv, isGlobal bool, args []string) int {
	updates, err := splitKeyValPairs(args)

	if err != nil {
		fmt.Fprintln(os.Stderr, color.RedString("Invalid argument format.  Usage: dolt config [-local|config] -set key1:value1 ... keyN:valueN"))
		return 1
	}

	if cfg, ok := cliEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
		if !isGlobal {
			err = cliEnv.Config.CreateLocalConfig(updates)

			if err != nil {
				fmt.Fprintln(os.Stderr, color.RedString("Unable to create repo local config file"))
				return 1
			}

		} else {
			panic("Should not have been able to get this far without a global config.")
		}
	} else {
		err = cfg.SetStrings(updates)

		if err != nil {
			fmt.Fprintln(os.Stderr, color.RedString("Failed to update config."))
			return 1
		}
	}

	fmt.Println(color.CyanString("Config successfully updated."))
	return 0
}

func splitKeyValPairs(args []string) (map[string]string, error) {
	kvps := make(map[string]string)

	if kvps != nil {
		for _, arg := range args {
			colon := strings.IndexByte(arg, ':')

			if colon == -1 {
				return nil, errors.New(arg + "is not in the format key:value")
			}

			key := arg[:colon]
			value := arg[colon+1:]
			kvps[key] = value
		}
	}

	return kvps, nil
}

func unsetOperation(cliEnv *env.DoltCLIEnv, isGlobal bool, args []string) int {
	if cfg, ok := cliEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
		fmt.Fprintln(os.Stderr, color.RedString("Unable to read config."))
		return 1
	} else {
		if len(args) > 0 {
			err := cfg.Unset(args)

			if err != nil {
				fmt.Fprintln(os.Stderr, color.RedString("Error unsetting the keys %v. Error: %s", args, err.Error()))
				return 1
			}
		}

		fmt.Println(color.CyanString("Config successfully updated."))
		return 0
	}
}

func listOperation(cliEnv *env.DoltCLIEnv, isGlobal bool, printFn func(string, string)) int {
	if cfg, ok := cliEnv.Config.GetConfig(newCfgElement(isGlobal)); !ok {
		fmt.Fprintln(os.Stderr, color.RedString("Unable to read config."))
		return 1
	} else {
		cfg.Iter(func(name string, val string) (stop bool) {
			printFn(name, val)

			return false
		})

		return 0
	}
}

func newCfgElement(isGlobal bool) env.DoltConfigElement {
	if isGlobal {
		return env.GlobalConfig
	}

	return env.LocalConfig
}
