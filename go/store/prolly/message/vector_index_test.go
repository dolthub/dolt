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

package message

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/pool"
)

func TestVectorIndexMetadataRoundTrip(t *testing.T) {
	tests := []struct {
		distanceType vector.DistanceType
		expected     vector.DistanceType
	}{
		{vector.DistanceL2Squared{}, vector.DistanceL2Squared{}},
		// DistanceEuclidean produces the same ordering as DistanceL2Squared, so it is stored as L2_Squared.
		{vector.DistanceEuclidean{}, vector.DistanceL2Squared{}},
		{vector.DistanceCosine{}, vector.DistanceCosine{}},
		{vector.DistanceInnerProduct{}, vector.DistanceInnerProduct{}},
		{vector.DistanceL1{}, vector.DistanceL1{}},
	}
	for _, test := range tests {
		t.Run(test.distanceType.String(), func(t *testing.T) {
			logChunkSize := uint8(8)
			serializer := NewVectorIndexSerializer(pool.NewBuffPool(), logChunkSize, test.distanceType)
			msg := serializer.Serialize(nil, nil, nil, 0)
			distanceType, size, err := GetVectorIndexMetadata(msg)
			require.NoError(t, err)
			require.Equal(t, test.expected, distanceType)
			require.Equal(t, logChunkSize, size)
		})
	}
}

func TestEnumToDistanceType(t *testing.T) {
	// Vector index nodes written before the distance_type field existed read back as L2_Squared.
	distanceType, err := enumToDistanceType(serial.DistanceTypeNull)
	require.NoError(t, err)
	require.Equal(t, vector.DistanceL2Squared{}, distanceType)

	_, err = enumToDistanceType(serial.DistanceType(99))
	require.Error(t, err)
}
