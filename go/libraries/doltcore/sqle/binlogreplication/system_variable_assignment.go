package binlogreplication

import (
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
)

// assignSystemVariables binds variable callbacks to the supplied owning context.
func assignSystemVariables(ctx *sql.Context, vars sql.SystemVariableRegistry, values map[string]any) error {
	if ctx == nil || ctx.EngineVariables() == nil || vars == nil {
		return fmt.Errorf("binlog variable registry does not match the execution context")
	}
	sessionOwner, ok := ctx.Session.(sql.EngineVariablesProvider)
	if !ok || sessionOwner.EngineVariables() != ctx.EngineVariables() {
		return fmt.Errorf("binlog variable assignment requires a session with the context's owner")
	}
	aware, ok := vars.(sql.ContextAwareSystemVariableRegistry)
	if !ok {
		return fmt.Errorf("binlog variable registry %T does not support context-aware assignment", vars)
	}
	owned := ctx.EngineVariables().System
	if owned == nil || reflect.TypeOf(owned) != reflect.TypeOf(vars) || !reflect.TypeOf(vars).Comparable() || owned != vars {
		return fmt.Errorf("binlog variable registry does not match the execution context")
	}
	return aware.AssignValuesWithContext(ctx, values)
}
