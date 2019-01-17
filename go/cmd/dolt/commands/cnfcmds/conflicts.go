package cnfcmds

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
)

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "cat", Desc: "Writes out the table conflicts.", Func: Cat, ReqRepo: true},
	{Name: "resolve", Desc: "Removes rows from list of conflicts", Func: Resolve, ReqRepo: true},
})
