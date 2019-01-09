package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
)

func Version(version string) cli.CommandFunc {
	return func(commandStr string, args []string, dEnv *env.DoltEnv) int {
		cli.Println("The current dolt version is", version)

		return 0
	}
}
