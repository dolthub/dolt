package binlogreplication

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// assignSystemVariables binds variable callbacks to the supplied context when
// the registry supports context-aware assignments.
func assignSystemVariables(ctx *sql.Context, vars sql.SystemVariableRegistry, values map[string]any) error {
	if ctx.SystemVariables() != vars {
		return fmt.Errorf("binlog variable registry does not match the execution context")
	}
	if aware, ok := vars.(sql.ContextAwareSystemVariableRegistry); ok {
		return aware.AssignValuesWithContext(ctx, values)
	}
	return vars.AssignValues(values)
}
