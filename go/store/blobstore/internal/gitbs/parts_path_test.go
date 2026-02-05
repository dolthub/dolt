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

package gitbs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartPath_Deterministic(t *testing.T) {
	oid := "0123456789abcdef0123456789abcdef01234567"
	p, err := PartPath(oid)
	require.NoError(t, err)
	require.Equal(t, "__dolt_blobstore_parts__/01/23/"+oid, p)
}

func TestPartPath_NormalizesToLower(t *testing.T) {
	oidUpper := "0123456789ABCDEF0123456789ABCDEF01234567"
	p, err := PartPath(oidUpper)
	require.NoError(t, err)
	require.Equal(t, "__dolt_blobstore_parts__/01/23/0123456789abcdef0123456789abcdef01234567", p)
}

func TestPartPath_InvalidOID(t *testing.T) {
	_, err := PartPath("nope")
	require.Error(t, err)
}
