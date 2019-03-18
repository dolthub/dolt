package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
)

// Version displays the version of the running dolt client
func Version(version string) cli.CommandFunc {
	return func(commandStr string, args []string, dEnv *env.DoltEnv) int {
		cli.Println("dolt version", version)

		return 0
	}
}
