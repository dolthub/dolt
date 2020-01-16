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

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/fatih/color"
	"github.com/pkg/profile"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/cnfcmds"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/credcmds"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/schcmds"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/tblcmds"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

const (
	Version = "0.12.0"
)

var dumpDocsCommand = &commands.DumpDocsCmd{}
var doltCommand = cli.NewSubCommandHandler("dolt", "it's git for data", []cli.Command{
	commands.InitCmd{},
	commands.StatusCmd{},
	commands.AddCmd{},
	commands.ResetCmd{},
	commands.CommitCmd{},
	commands.SqlCmd{},
	sqlserver.SqlServerCmd{},
	commands.LogCmd{},
	commands.DiffCmd{},
	commands.BlameCmd{},
	commands.MergeCmd{},
	commands.BranchCmd{},
	commands.CheckoutCmd{},
	commands.RemoteCmd{},
	commands.PushCmd{},
	commands.PullCmd{},
	commands.FetchCmd{},
	commands.CloneCmd{},
	credcmds.Commands,
	commands.LoginCmd{},
	commands.VersionCmd{},
	commands.ConfigCmd{},
	commands.LsCmd{},
	schcmds.Commands,
	tblcmds.Commands,
	cnfcmds.Commands,
	commands.SendMetricsCmd{},
	dumpDocsCommand,
})

func init() {
	dumpDocsCommand.DoltCommand = doltCommand
}

const chdirFlag = "--chdir"
const profFlag = "--prof"
const cpuProf = "cpu"
const memProf = "mem"
const blockingProf = "blocking"
const traceProf = "trace"

func main() {
	os.Exit(runMain())
}

func runMain() int {
	args := os.Args[1:]

	if len(args) > 0 {
		var doneDebugFlags bool
		for !doneDebugFlags {
			switch args[0] {
			case profFlag:
				switch args[1] {
				case cpuProf:
					fmt.Println("cpu profiling enabled.")
					defer profile.Start(profile.CPUProfile).Stop()
				case memProf:
					fmt.Println("mem profiling enabled.")
					defer profile.Start(profile.MemProfile).Stop()
				case blockingProf:
					fmt.Println("block profiling enabled")
					defer profile.Start(profile.BlockProfile).Stop()
				case traceProf:
					fmt.Println("trace profiling enabled")
					defer profile.Start(profile.TraceProfile).Stop()
				default:
					panic("Unexpected prof flag: " + args[1])
				}
				args = args[2:]

			// Currently goland doesn't support running with a different working directory when using go modules.
			// This is a hack that allows a different working directory to be set after the application starts using
			// chdir=<DIR>.  The syntax is not flexible and must match exactly this.
			case chdirFlag:
				err := os.Chdir(args[1])

				if err != nil {
					panic(err)
				}

				args = args[2:]

			default:
				doneDebugFlags = true
			}
		}
	}

	restoreIO := cli.InitIO()
	defer restoreIO()

	warnIfMaxFilesTooLow()

	dEnv := env.Load(context.TODO(), env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB)

	root, err := env.GetCurrentUserHomeDir()

	if err != nil {
		return 1
	}

	emitter := events.NewFileEmitter(root, dbfactory.DoltDir)

	defer func() {
		ces := events.GlobalCollector.Close()
		// events.WriterEmitter{cli.CliOut}.LogEvents(Version, ces)

		metricsDisabled := dEnv.Config.GetStringOrDefault(env.MetricsDisabled, "false")

		disabled, err := strconv.ParseBool(*metricsDisabled)
		if err != nil {
			// log.Print(err)
			return
		}

		if disabled {
			return
		}

		// write events
		_ = emitter.LogEvents(Version, ces)

		// flush events
		if err := processEventsDir(args, dEnv); err != nil {
			// log.Print(err)
		}
	}()

	if dEnv.CfgLoadErr != nil {
		cli.PrintErrln(color.RedString("Failed to load the global config.", dEnv.CfgLoadErr))
		return 1
	}

	return doltCommand.Exec(context.Background(), "dolt", args, dEnv)
}

// processEventsDir runs the dolt send-metrics command in a new process
func processEventsDir(args []string, dEnv *env.DoltEnv) error {
	if len(args) > 0 {
		ignoreCommands := map[string]struct{}{
			commands.SendMetricsCommand: {},
			"init":                      {},
			"config":                    {},
		}

		_, ok := ignoreCommands[args[0]]

		if ok {
			return nil
		}

		cmd := exec.Command("dolt", commands.SendMetricsCommand)

		if err := cmd.Start(); err != nil {
			// log.Print(err)
			return err
		}

		return nil
	}

	return nil
}
