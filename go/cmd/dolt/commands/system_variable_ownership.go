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
