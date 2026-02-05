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

// Package gitbs contains internal helpers for GitBlobstore representations.
//
// This package is intentionally Git-agnostic: it does not import the internal/git
// plumbing, and does not assume any ref/update strategy. It focuses on chunked
// object descriptor encoding/decoding and validation.
package gitbs

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

const (
	// DescriptorMagic is the first line of a chunked-object descriptor.
	DescriptorMagic = "DOLTBS1"
)

type Descriptor struct {
	TotalSize uint64
	Parts     []PartRef
}

type PartRef struct {
	OIDHex string
	Size   uint64
}

type descriptorParseState struct {
	d       Descriptor
	haveSz  bool
	sumPart uint64
}

// IsDescriptorPrefix returns true if |b| looks like the beginning of a descriptor.
// Callers can use this on a small prefix before deciding whether to read and parse
// the full blob.
func IsDescriptorPrefix(b []byte) bool {
	// Be conservative: require the magic line break plus "size " prefix.
	// This avoids mis-detecting arbitrary inline content that begins with "DOLTBS1".
	if !bytes.HasPrefix(b, []byte(DescriptorMagic)) {
		return false
	}
	if len(b) < len(DescriptorMagic)+1 {
		return false
	}
	rest := b[len(DescriptorMagic):]
	if bytes.HasPrefix(rest, []byte("\nsize ")) {
		return true
	}
	if bytes.HasPrefix(rest, []byte("\r\nsize ")) {
		return true
	}
	return false
}

// ParseDescriptor parses and validates a descriptor blob.
func ParseDescriptor(b []byte) (Descriptor, error) {
	lines := splitLines(string(b))
	if len(lines) == 0 {
		return Descriptor{}, fmt.Errorf("descriptor: empty")
	}
	if lines[0] != DescriptorMagic {
		return Descriptor{}, fmt.Errorf("descriptor: invalid magic %q", lines[0])
	}

	var st descriptorParseState
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if err := parseDescriptorLine(&st, line); err != nil {
			return Descriptor{}, err
		}
	}
	return finalizeParsedDescriptor(st)
}

// EncodeDescriptor encodes a descriptor in the stable line-oriented format.
func EncodeDescriptor(d Descriptor) ([]byte, error) {
	// Validate basic invariants so Encode+Parse is deterministic.
	if _, err := validateDescriptorForEncode(d); err != nil {
		return nil, err
	}

	var buf strings.Builder
	buf.Grow(64 + len(d.Parts)*64)
	buf.WriteString(DescriptorMagic)
	buf.WriteByte('\n')
	buf.WriteString("size ")
	buf.WriteString(strconv.FormatUint(d.TotalSize, 10))
	buf.WriteByte('\n')
	for _, p := range d.Parts {
		writePartLine(&buf, p)
	}
	return []byte(buf.String()), nil
}

func validateDescriptorForEncode(d Descriptor) (Descriptor, error) {
	sum, err := validateDescriptorParts(d.Parts)
	if err != nil {
		return Descriptor{}, err
	}
	if err := validateDescriptorSizeAndParts(d.TotalSize, len(d.Parts), sum); err != nil {
		return Descriptor{}, err
	}
	return d, nil
}

func parseDescriptorLine(st *descriptorParseState, line string) error {
	fields := strings.Fields(line)
	switch {
	case len(fields) >= 1 && fields[0] == "size":
		return parseSizeLine(st, line, fields)
	case len(fields) >= 1 && fields[0] == "part":
		return parsePartLine(st, line, fields)
	default:
		return fmt.Errorf("descriptor: unknown line %q", line)
	}
}

func parseSizeLine(st *descriptorParseState, line string, fields []string) error {
	if st.haveSz {
		return fmt.Errorf("descriptor: multiple size lines")
	}
	if len(fields) != 2 {
		return fmt.Errorf("descriptor: malformed size line %q", line)
	}
	n, err := parseUint(fields[1])
	if err != nil {
		return fmt.Errorf("descriptor: invalid size %q: %w", fields[1], err)
	}
	st.d.TotalSize = n
	st.haveSz = true
	return nil
}

func parsePartLine(st *descriptorParseState, line string, fields []string) error {
	if len(fields) != 3 {
		return fmt.Errorf("descriptor: malformed part line %q", line)
	}
	oid := fields[1]
	if err := validateOIDHex(oid); err != nil {
		return fmt.Errorf("descriptor: invalid part oid %q: %w", oid, err)
	}
	sz, err := parseUint(fields[2])
	if err != nil {
		return fmt.Errorf("descriptor: invalid part size %q: %w", fields[2], err)
	}
	if sz == 0 {
		return fmt.Errorf("descriptor: part size must be > 0")
	}
	if st.sumPart > ^uint64(0)-sz {
		return fmt.Errorf("descriptor: part sizes overflow uint64")
	}
	st.sumPart += sz
	st.d.Parts = append(st.d.Parts, PartRef{OIDHex: oid, Size: sz})
	return nil
}

func finalizeParsedDescriptor(st descriptorParseState) (Descriptor, error) {
	if !st.haveSz {
		return Descriptor{}, fmt.Errorf("descriptor: missing size line")
	}
	if err := validateDescriptorSizeAndParts(st.d.TotalSize, len(st.d.Parts), st.sumPart); err != nil {
		return Descriptor{}, err
	}
	return st.d, nil
}

func validateDescriptorSizeAndParts(totalSize uint64, partCount int, sumParts uint64) error {
	if totalSize == 0 {
		if partCount != 0 {
			return fmt.Errorf("descriptor: total size 0 requires zero parts")
		}
		return nil
	}
	if partCount == 0 {
		return fmt.Errorf("descriptor: non-zero total size requires at least one part")
	}
	if sumParts != totalSize {
		return fmt.Errorf("descriptor: part sizes sum to %d, expected %d", sumParts, totalSize)
	}
	return nil
}

func validateDescriptorParts(parts []PartRef) (sum uint64, err error) {
	for _, p := range parts {
		if err := validateOIDHex(p.OIDHex); err != nil {
			return 0, fmt.Errorf("descriptor: invalid part oid %q: %w", p.OIDHex, err)
		}
		if p.Size == 0 {
			return 0, fmt.Errorf("descriptor: part size must be > 0")
		}
		if sum > ^uint64(0)-p.Size {
			return 0, fmt.Errorf("descriptor: part sizes overflow uint64")
		}
		sum += p.Size
	}
	return sum, nil
}

func writePartLine(buf *strings.Builder, p PartRef) {
	buf.WriteString("part ")
	buf.WriteString(p.OIDHex)
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatUint(p.Size, 10))
	buf.WriteByte('\n')
}

func splitLines(s string) []string {
	// Normalize CRLF to LF, then split.
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func parseUint(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}
