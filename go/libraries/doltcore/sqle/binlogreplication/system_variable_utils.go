// Copyright 2024 Dolthub, Inc.
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
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// getServerId returns the @@server_id global system variable value. If the value of @@server_id is 0 or is not a
// uint32 value, then an error is returned.
func getServerId(ctx *sql.Context) (uint32, error) {
	_, value, ok := sql.SystemVariables.GetGlobal("server_id")
	if !ok {
		return 0, fmt.Errorf("global variable 'server_id' not found")
	}

	// Attempt to convert the server_id value into a UINT32, in case it has been loaded as a string
	// through global JSON configuration.
	convertedValue, _, err := types.Uint32.Convert(ctx, value)
	if err != nil {
		return 0, err
	}

	if i, ok := convertedValue.(uint32); ok {
		if i == 0 {
			return 0, fmt.Errorf("@@server_id is zero – must be set to a non-zero value")
		}
		return i, nil
	}

	return 0, fmt.Errorf("@@server_id is not a valid uint32 – must be set to a non-zero value")
}

// getServerUuid returns the global @@server_uuid system variable value. If the value of @@server_uuid
// is empty or is not a string, then an error is returned.
func getServerUuid(_ context.Context) (string, error) {
	_, serverUuidValue, ok := sql.SystemVariables.GetGlobal("server_uuid")
	if !ok {
		return "", fmt.Errorf("global variable 'server_uuid' not found")
	}

	if s, ok := serverUuidValue.(string); ok {
		if len(s) == 0 {
			return "", fmt.Errorf("@@server_uuid is empty – must be set to a valid UUID")
		}
		return s, nil
	}

	return "", fmt.Errorf("@@server_uuid is not a string – must be set to a valid UUID")
}
