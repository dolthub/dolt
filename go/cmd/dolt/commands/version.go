package commands

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
)

func Version(version string) cli.CommandFunc {
	return func(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
		fmt.Println("The current dolt version is", version)

		return 0
	}
}
