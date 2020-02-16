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
)

//TODO: add some basic tests once the storage format has been decided

func generateSetTypes(t *testing.T, numOfTypes int64) []TypeInfo {
	res := make([]TypeInfo, numOfTypes)
	for i := int64(1); i <= numOfTypes; i++ {
		res[i-1] = generateSetType(t, int(i))
	}
	return res
}

func generateSetType(t *testing.T, numOfElements int) *setType {
	require.True(t, numOfElements >= 1 && numOfElements <= sql.SetTypeMaxElements)
	vals := make([]string, numOfElements)
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	lenAlphabet := len(alphabet)
	for i := 0; i < numOfElements; i++ {
		vals[i] = string([]byte{alphabet[(i/lenAlphabet)%lenAlphabet], alphabet[i%lenAlphabet]})
	}
	return &setType{sql.MustCreateSetType(vals, sql.Collation_Default)}
}
