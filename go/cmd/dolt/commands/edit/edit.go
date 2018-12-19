package edit

import "github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "create", Desc: "Creates or overwrites a table from the data in a file.", Func: Create, ReqRepo: true},
	{Name: "update", Desc: "Updates a table from the data in a file.", Func: Update, ReqRepo: true},
	{Name: "put-row", Desc: "Add a row to a table.", Func: PutRow, ReqRepo: true},
	{Name: "rm-row", Desc: "Remove a row from a table.", Func: RmRow, ReqRepo: true},
	{Name: "export", Desc: "Export a table to a file.", Func: Export, ReqRepo: true},
})
