package commands

import (
	"os"

	"github.com/dolthub/go-mysql-server/sql"
)

func restrictLoadFileToWorkingDirectory(ctx *sql.Context) error {
	wd, err := os.Getwd()
	if err != nil {
		wd = "/dev/null"
	}
	vars := ctx.SystemVariables()
	values := map[string]interface{}{"secure_file_priv": wd}
	if aware, ok := vars.(sql.ContextAwareSystemVariableRegistry); ok {
		return aware.AssignValuesWithContext(ctx, values)
	}
	return vars.AssignValues(values)
}
