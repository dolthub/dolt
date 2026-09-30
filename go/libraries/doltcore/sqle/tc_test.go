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

package sqle

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXIDFromUUIDv7(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	u := uuid.MustParse("0191b000-0000-7000-8000-000000000001")
	expected := binary.BigEndian.Uint64(u[0:8])

	xid := XIDFromUUIDv7(u)
	assert.Equal(t, expected, xid)
	assert.NotZero(t, xid)
}

func TestNextXID(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	xid1, u1, err := NextXID()
	require.NoError(t, err)
	require.NotZero(t, xid1)
	require.NotEqual(t, uuid.Nil, u1)

	time.Sleep(2 * time.Millisecond)

	xid2, u2, err := NextXID()
	require.NoError(t, err)
	require.NotZero(t, xid2)
	require.NotEqual(t, uuid.Nil, u2)

	assert.NotEqual(t, u1, u2)
	assert.True(t, xid2 > xid1, "XID derived from UUIDv7 must be monotonically increasing")
}

func TestXIDFromUUIDv7_NilUUID(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	xid := XIDFromUUIDv7(uuid.Nil)
	assert.Equal(t, uint64(0), xid)
}

func TestHasXID_DefaultUnregistered(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/11533
	ResetXIDCheckerForTesting()
	assert.False(t, HasXID(nil, 12345))
}

