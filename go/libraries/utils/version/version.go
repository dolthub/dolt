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

func Encode(version string) (float64, error) {
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
	return float64(versionUint32), nil
}

func Decode(version float64) string {
	versInt32 := uint32(version)
	major := (versInt32 & 0xFF000000) >> 24
	minor := (versInt32 & 0x00FF0000) >> 16
	build := versInt32 & 0x0000FFFF

	majorStr := strconv.FormatUint(uint64(major), 10)
	minorStr := strconv.FormatUint(uint64(minor), 10)
	buildStr := strconv.FormatUint(uint64(build), 10)

	return majorStr + "." + minorStr + "." + buildStr
}
