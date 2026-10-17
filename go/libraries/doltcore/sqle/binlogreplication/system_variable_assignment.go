// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
