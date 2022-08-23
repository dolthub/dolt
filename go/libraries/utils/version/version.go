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
	"fmt"
	"strconv"
	"strings"
)

// Encode encodes a version string in the format "X.Y.Z" into a uint32 (X represents the major version and must be
// numeric and in the range 0-255, Y represents the minor version and must be in the range 0-255, and Z represents the
// build which is in the range 0-65535). The encoded uint32 version uses the highest 8 bits for the major version,
// the next 8 bits for the minor version, and the last 16 used for the build number. Encoded versions are numerically
// comparable (as in the encoded value of the version 1.2.3 will be less than the encoded value of the version 1.11.0)
func Encode(version string) (uint32, error) {
	parts := strings.Split(version, ".")

	if len(parts) != 3 {
		return 0, fmt.Errorf("version '%s' is not in the format X.X.X", version)
	}

	partVals := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		var err error
		partVals[i], err = strconv.ParseUint(parts[i], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("failed to parse version '%s'. error at '%s': %w", version, parts[i], err)
		}
	}

	if partVals[0] > 255 || partVals[1] > 255 || partVals[2] > 65535 {
		return 0, fmt.Errorf("version '%s' cannot be encoded with 8 bits for major, 8 bits for minor, 16 bits for build", version)
	}

	versionUint32 := (uint32(partVals[0]&0xFF) << 24) | (uint32(partVals[1]&0xFF) << 16) | uint32(partVals[2]&0xFFFF)
	return versionUint32, nil
}

// Decode converts the uint32 encoding of the version (described by the Encode method) back to its string representation.
func Decode(version uint32) string {
	major := (version & 0xFF000000) >> 24
	minor := (version & 0x00FF0000) >> 16
	build := version & 0x0000FFFF

	majorStr := strconv.FormatUint(uint64(major), 10)
	minorStr := strconv.FormatUint(uint64(minor), 10)
	buildStr := strconv.FormatUint(uint64(build), 10)

	return majorStr + "." + minorStr + "." + buildStr
}
