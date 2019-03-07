package tblcmds

import "github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"

var Commands = cli.GenSubCommandHandler([]*cli.Command{
	{Name: "import", Desc: "Creates, overwrites, or updates a table from the data in a file.", Func: Import, ReqRepo: true},
	{Name: "export", Desc: "Export a table to a file.", Func: Export, ReqRepo: true},
	{Name: "create", Desc: "Creates or overwrite an existing table with an empty table.", Func: Create, ReqRepo: true},
	{Name: "rm", Desc: "Deletes a table", Func: Rm, ReqRepo: true},
	{Name: "mv", Desc: "Moves a table", Func: Mv, ReqRepo: true},
	{Name: "cp", Desc: "Copies a table", Func: Cp, ReqRepo: true},
	{Name: "select", Desc: "Print a selection of a table.", Func: Select, ReqRepo: true},
	{Name: "schema", Desc: "Display the schema for table(s)", Func: Schema, ReqRepo: true},
	{Name: "put-row", Desc: "Add a row to a table.", Func: PutRow, ReqRepo: true},
	{Name: "rm-row", Desc: "Remove a row from a table.", Func: RmRow, ReqRepo: true},
})
