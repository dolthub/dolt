// Copyright 2020 Liquidata, Inc.
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

package typeinfo

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
)

//TODO: add some basic tests once the storage format has been decided

func generateVarBinaryTypes(t *testing.T, numOfTypes uint16) []TypeInfo {
	var res []TypeInfo
	loop(t, 1, 500, numOfTypes, func(i int64) {
		pad := false
		if i%2 == 0 {
			pad = true
		}
		res = append(res, generateVarBinaryType(t, i, pad))
	})
	return res
}

func generateVarBinaryType(t *testing.T, length int64, pad bool) *varBinaryType {
	require.True(t, length > 0)
	if pad {
		t, err := sql.CreateBinary(sqltypes.Binary, length)
		if err == nil {
			return &varBinaryType{t}
		}
	}
	return &varBinaryType{sql.MustCreateBinary(sqltypes.VarBinary, length)}
}
