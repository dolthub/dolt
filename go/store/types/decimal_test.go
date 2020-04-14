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

package types

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestDecimalLibraryEncoding(t *testing.T) {
	expectedBytes := []byte{255, 255, 255, 250, 3, 25, 222, 110, 95, 84, 132}
	dec := decimal.RequireFromString("-28443125.175428")
	bytes, err := dec.GobEncode()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, bytes)
	expectedDec := decimal.Decimal{}
	err = expectedDec.GobDecode(expectedBytes)
	require.NoError(t, err)
	require.True(t, expectedDec.Equal(dec))
}
