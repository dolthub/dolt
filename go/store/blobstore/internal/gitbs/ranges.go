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

import "fmt"

// PartSlice describes a contiguous slice to read from a particular part.
type PartSlice struct {
	OIDHex string
	// Offset is the byte offset into the part at which to begin reading.
	Offset int64
	// Length is the number of bytes to read from the part slice.
	Length int64
}

// NormalizeRange converts (offset,length) with possible negative offsets into a
// concrete half-open interval [start,end) over an object of total size |total|.
//
// Semantics match blobstore.BlobRange:
// - offset < 0 means relative to end (start = total + offset)
// - length == 0 means "to end"
// - length < 0 is invalid
func NormalizeRange(total int64, offset int64, length int64) (start, end int64, err error) {
	if total < 0 {
		return 0, 0, fmt.Errorf("invalid total size %d", total)
	}
	if length < 0 {
		return 0, 0, fmt.Errorf("invalid length %d", length)
	}
	start = offset
	if start < 0 {
		start = total + start
	}
	if start < 0 || start > total {
		return 0, 0, fmt.Errorf("invalid offset %d for total size %d", offset, total)
	}
	if length == 0 {
		end = total
	} else {
		end = start + length
		if end < start {
			return 0, 0, fmt.Errorf("range overflow")
		}
		if end > total {
			end = total
		}
	}
	return start, end, nil
}

// SliceParts maps a logical range [start,end) over the concatenation of |parts|
// into per-part slices.
//
// - start/end are byte offsets in the logical object (0 <= start <= end <= total)
// - parts must have Size > 0
func SliceParts(parts []PartRef, start, end int64) ([]PartSlice, error) {
	if start < 0 || end < 0 || end < start {
		return nil, fmt.Errorf("invalid start/end: %d/%d", start, end)
	}
	if start == end {
		return nil, nil
	}

	var (
		out []PartSlice
		pos int64 // start offset of current part in logical stream
	)

	for _, p := range parts {
		if p.Size == 0 {
			return nil, fmt.Errorf("invalid part size 0")
		}
		partStart := pos
		partEnd := pos + int64(p.Size)
		if partEnd < partStart {
			return nil, fmt.Errorf("part size overflow")
		}

		// Does this part overlap [start,end)?
		if end <= partStart {
			break
		}
		if start >= partEnd {
			pos = partEnd
			continue
		}

		// Compute overlap.
		s := start
		if s < partStart {
			s = partStart
		}
		e := end
		if e > partEnd {
			e = partEnd
		}
		if e > s {
			out = append(out, PartSlice{
				OIDHex: p.OIDHex,
				Offset: s - partStart,
				Length: e - s,
			})
		}
		pos = partEnd
	}

	// Validate that the requested interval was fully covered by parts.
	if len(out) == 0 {
		return nil, fmt.Errorf("range [%d,%d) not covered by parts", start, end)
	}
	var covered int64
	for _, s := range out {
		covered += s.Length
	}
	if covered != (end - start) {
		return nil, fmt.Errorf("range [%d,%d) not fully covered by parts", start, end)
	}
	return out, nil
}
