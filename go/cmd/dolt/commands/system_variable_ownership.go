package commands

import (
	"fmt"
	"os"

	"github.com/dolthub/go-mysql-server/sql"
)

func restrictLoadFileToWorkingDirectory(ctx *sql.Context) error {
	if ctx == nil || ctx.EngineVariables() == nil {
		return fmt.Errorf("LOAD_FILE restriction requires an owning context")
	}
	sessionOwner, ok := ctx.Session.(sql.EngineVariablesProvider)
	if !ok || sessionOwner.EngineVariables() != ctx.EngineVariables() {
		return fmt.Errorf("LOAD_FILE restriction requires a session with the context's owner")
	}
	wd, err := os.Getwd()
	if err != nil {
		wd = "/dev/null"
	}
	vars := ctx.SystemVariables()
	values := map[string]interface{}{"secure_file_priv": wd}
	aware, ok := vars.(sql.ContextAwareSystemVariableRegistry)
	if !ok {
		return fmt.Errorf("system variable registry %T does not support context-aware assignment", vars)
	}
	return aware.AssignValuesWithContext(ctx, values)
}
