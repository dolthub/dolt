package binlogreplication

import (
	"fmt"
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
)

// assignSystemVariables binds variable callbacks to the supplied owning context.
func assignSystemVariables(ctx *sql.Context, vars sql.SystemVariableRegistry, values map[string]any) error {
	if ctx == nil || vars == nil {
		return fmt.Errorf("binlog variable registry does not match the execution context")
	}
	if ctx.EngineVariables() == nil {
		// Unbound contexts belong to engines sharing the global registry.
		if !sameRegistry(vars, sql.SystemVariables) {
			return fmt.Errorf("binlog variable registry does not match the execution context")
		}
		return vars.AssignValues(values)
	}
	sessionOwner, ok := ctx.Session.(sql.EngineVariablesProvider)
	if !ok || sessionOwner.EngineVariables() != ctx.EngineVariables() {
		return fmt.Errorf("binlog variable assignment requires a session with the context's owner")
	}
	aware, ok := vars.(sql.ContextAwareSystemVariableRegistry)
	if !ok {
		return fmt.Errorf("binlog variable registry %T does not support context-aware assignment", vars)
	}
	if !sameRegistry(ctx.EngineVariables().System, vars) {
		return fmt.Errorf("binlog variable registry does not match the execution context")
	}
	return aware.AssignValuesWithContext(ctx, values)
}

// sameRegistry compares registries without panicking on uncomparable implementations.
func sameRegistry(a, b sql.SystemVariableRegistry) bool {
	return a != nil && b != nil && reflect.TypeOf(a) == reflect.TypeOf(b) && reflect.TypeOf(a).Comparable() && a == b
}
