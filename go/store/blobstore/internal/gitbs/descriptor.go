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

	var (
		d       Descriptor
		haveSz  bool
		sumPart uint64
	)

	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		switch {
		case len(fields) >= 1 && fields[0] == "size":
			if haveSz {
				return Descriptor{}, fmt.Errorf("descriptor: multiple size lines")
			}
			if len(fields) != 2 {
				return Descriptor{}, fmt.Errorf("descriptor: malformed size line %q", line)
			}
			n, err := parseUint(fields[1])
			if err != nil {
				return Descriptor{}, fmt.Errorf("descriptor: invalid size %q: %w", fields[1], err)
			}
			d.TotalSize = n
			haveSz = true

		case len(fields) >= 1 && fields[0] == "part":
			if len(fields) != 3 {
				return Descriptor{}, fmt.Errorf("descriptor: malformed part line %q", line)
			}
			oid := fields[1]
			if err := validateOIDHex(oid); err != nil {
				return Descriptor{}, fmt.Errorf("descriptor: invalid part oid %q: %w", oid, err)
			}
			sz, err := parseUint(fields[2])
			if err != nil {
				return Descriptor{}, fmt.Errorf("descriptor: invalid part size %q: %w", fields[2], err)
			}
			if sz == 0 {
				return Descriptor{}, fmt.Errorf("descriptor: part size must be > 0")
			}
			if sumPart > ^uint64(0)-sz {
				return Descriptor{}, fmt.Errorf("descriptor: part sizes overflow uint64")
			}
			sumPart += sz
			d.Parts = append(d.Parts, PartRef{OIDHex: oid, Size: sz})

		default:
			return Descriptor{}, fmt.Errorf("descriptor: unknown line %q", line)
		}
	}

	if !haveSz {
		return Descriptor{}, fmt.Errorf("descriptor: missing size line")
	}
	if d.TotalSize == 0 {
		if len(d.Parts) != 0 {
			return Descriptor{}, fmt.Errorf("descriptor: total size 0 requires zero parts")
		}
		return d, nil
	}
	if len(d.Parts) == 0 {
		return Descriptor{}, fmt.Errorf("descriptor: non-zero total size requires at least one part")
	}
	if sumPart != d.TotalSize {
		return Descriptor{}, fmt.Errorf("descriptor: part sizes sum to %d, expected %d", sumPart, d.TotalSize)
	}
	return d, nil
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
		buf.WriteString("part ")
		buf.WriteString(p.OIDHex)
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(p.Size, 10))
		buf.WriteByte('\n')
	}
	return []byte(buf.String()), nil
}

func validateDescriptorForEncode(d Descriptor) (Descriptor, error) {
	var sum uint64
	if d.TotalSize == 0 {
		if len(d.Parts) != 0 {
			return Descriptor{}, fmt.Errorf("descriptor: total size 0 requires zero parts")
		}
		return d, nil
	}
	if len(d.Parts) == 0 {
		return Descriptor{}, fmt.Errorf("descriptor: non-zero total size requires at least one part")
	}
	for _, p := range d.Parts {
		if err := validateOIDHex(p.OIDHex); err != nil {
			return Descriptor{}, fmt.Errorf("descriptor: invalid part oid %q: %w", p.OIDHex, err)
		}
		if p.Size == 0 {
			return Descriptor{}, fmt.Errorf("descriptor: part size must be > 0")
		}
		if sum > ^uint64(0)-p.Size {
			return Descriptor{}, fmt.Errorf("descriptor: part sizes overflow uint64")
		}
		sum += p.Size
	}
	if sum != d.TotalSize {
		return Descriptor{}, fmt.Errorf("descriptor: part sizes sum to %d, expected %d", sum, d.TotalSize)
	}
	return d, nil
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

func validateOIDHex(oid string) error {
	if len(oid) != 40 {
		return fmt.Errorf("expected 40 hex chars, got %d", len(oid))
	}
	for i := 0; i < len(oid); i++ {
		c := oid[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return fmt.Errorf("non-hex character %q", c)
		}
	}
	return nil
}
