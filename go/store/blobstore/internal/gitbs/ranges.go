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
	if err := validateNormalizeRangeInputs(total, length); err != nil {
		return 0, 0, err
	}
	start, err = normalizeStart(total, offset)
	if err != nil {
		return 0, 0, err
	}
	end, err = normalizeEnd(total, start, length)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// SliceParts maps a logical range [start,end) over the concatenation of |parts|
// into per-part slices.
//
// - start/end are byte offsets in the logical object (0 <= start <= end <= total)
// - parts must have Size > 0
func SliceParts(parts []PartRef, start, end int64) ([]PartSlice, error) {
	if err := validateStartEnd(start, end); err != nil {
		return nil, err
	}
	if isEmptyRange(start, end) {
		return nil, nil
	}

	var (
		out []PartSlice
		pos int64 // start offset of current part in logical stream
	)

	for _, p := range parts {
		partStart, partEnd, err := partBounds(pos, p.Size)
		if err != nil {
			return nil, err
		}

		// Does this part overlap [start,end)?
		if end <= partStart {
			break
		}
		if start >= partEnd {
			pos = partEnd
			continue
		}

		if s, e, ok := overlap(partStart, partEnd, start, end); ok {
			out = append(out, newPartSlice(p.OIDHex, partStart, s, e))
		}
		pos = partEnd
	}

	return validateCoverage(out, start, end)
}

func validateNormalizeRangeInputs(total int64, length int64) error {
	if total < 0 {
		return fmt.Errorf("invalid total size %d", total)
	}
	if length < 0 {
		return fmt.Errorf("invalid length %d", length)
	}
	return nil
}

func normalizeStart(total int64, offset int64) (int64, error) {
	start := offset
	if start < 0 {
		start = total + start
	}
	if start < 0 || start > total {
		return 0, fmt.Errorf("invalid offset %d for total size %d", offset, total)
	}
	return start, nil
}

func normalizeEnd(total int64, start int64, length int64) (int64, error) {
	if length == 0 {
		return total, nil
	}
	end := start + length
	if end < start {
		return 0, fmt.Errorf("range overflow")
	}
	if end > total {
		end = total
	}
	return end, nil
}

func validateStartEnd(start, end int64) error {
	if start < 0 || end < 0 || end < start {
		return fmt.Errorf("invalid start/end: %d/%d", start, end)
	}
	return nil
}

func isEmptyRange(start, end int64) bool {
	return start == end
}

func partBounds(pos int64, size uint64) (start, end int64, err error) {
	if size == 0 {
		return 0, 0, fmt.Errorf("invalid part size 0")
	}
	start = pos
	end = pos + int64(size)
	if end < start {
		return 0, 0, fmt.Errorf("part size overflow")
	}
	return start, end, nil
}

func overlap(partStart, partEnd, start, end int64) (s, e int64, ok bool) {
	s = start
	if s < partStart {
		s = partStart
	}
	e = end
	if e > partEnd {
		e = partEnd
	}
	if e <= s {
		return 0, 0, false
	}
	return s, e, true
}

func newPartSlice(oidHex string, partStart, s, e int64) PartSlice {
	return PartSlice{
		OIDHex: oidHex,
		Offset: s - partStart,
		Length: e - s,
	}
}

func validateCoverage(out []PartSlice, start, end int64) ([]PartSlice, error) {
	// Validate that the requested interval was fully covered by parts.
	if len(out) == 0 {
		return nil, fmt.Errorf("range [%d,%d) not covered by parts", start, end)
	}
	covered := coveredLength(out)
	if covered != (end - start) {
		return nil, fmt.Errorf("range [%d,%d) not fully covered by parts", start, end)
	}
	return out, nil
}

func coveredLength(slices []PartSlice) (covered int64) {
	for _, s := range slices {
		covered += s.Length
	}
	return covered
}
