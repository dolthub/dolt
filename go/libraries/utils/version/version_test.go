// Copyright 2022 Dolthub, Inc.
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

package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeVersion(t *testing.T) {
	versions := []string{
		"0.0.0",
		"1.2.3",
		"128.128.32768",
		"255.255.65535",
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			encoded, err := Encode(version)
			require.NoError(t, err)

			decoded := Decode(encoded)
			require.Equal(t, version, decoded)
		})
	}
}

func TestBadVersionEncodeFailure(t *testing.T) {
	versions := []string{
		"256.0.0",
		"0.256.0",
		"0.0.65536",
		"a.0.0",
		"0.40.256c",
		"-1.0.0",
		"2.0",
		"3.5.",
		"..",
	}

	for _, version := range versions {
		t.Run(version, func(t *testing.T) {
			_, err := Encode(version)
			require.Error(t, err)
		})
	}
}
