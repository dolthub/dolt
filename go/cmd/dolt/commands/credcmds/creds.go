package credcmds

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
)

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "new", Desc: "", Func: New, ReqRepo: false},
	{Name: "rm", Desc: "", Func: Rm, ReqRepo: false},
	{Name: "ls", Desc: "", Func: Ls, ReqRepo: false},
})
