// Copyright 2018 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dwarf

import (
	"fmt"
	"strconv"
)

// Parse the type units stored in a DWARF4 .debug_types section.  Each
// type unit defines a single primary type and an 8-byte signature.
// Other sections may then use formRefSig8 to refer to the type.

// The typeUnit format is a single type with a signature.  It holds
// the same data as a compilation unit.
type typeUnit struct {
	unit
	toff  Offset // Offset to signature type within data.
	name  string // Name of .debug_type section.
	cache Type   // Cache the type, nil to start.
}

// Parse a .debug_types section.
func (d *Data) parseTypes(name string, types []byte) error {
	b := makeBuf(d, unknownFormat{}, name, 0, types)
	for len(b.data) > 0 {
		base := b.off
		dwarf64 := false
		n := b.uint32()
		if n == 0xffffffff {
			n64 := b.uint64()
			if n64 != uint64(uint32(n64)) {
				b.error("type unit length overflow")
				return b.err
			}
			n = uint32(n64)
			dwarf64 = true
		}
		hdroff := b.off
		vers := b.uint16()
		if vers != 4 {
			b.error("unsupported DWARF version " + strconv.Itoa(int(vers)))
			return b.err
		}
		var ao uint32
		if !dwarf64 {
			ao = b.uint32()
		} else {
			ao64 := b.uint64()
			if ao64 != uint64(uint32(ao64)) {
				b.error("type unit abbrev offset overflow")
				return b.err
			}
			ao = uint32(ao64)
		}
		atable, err := d.parseAbbrev(ao)
		if err != nil {
			return err
		}
		asize := b.uint8()
		sig := b.uint64()

		var toff uint32
		if !dwarf64 {
			toff = b.uint32()
		} else {
			to64 := b.uint64()
			if to64 != uint64(uint32(to64)) {
				b.error("type unit type offset overflow")
				return b.err
			}
			toff = uint32(to64)
		}

		boff := b.off
		d.typeSigs[sig] = &typeUnit{
			unit: unit{
				base:   base,
				off:    boff,
				data:   b.bytes(int(Offset(n) - (b.off - hdroff))),
				atable: atable,
				asize:  int(asize),
				vers:   int(vers),
				is64:   dwarf64,
			},
			toff: Offset(toff),
			name: name,
		}
		if b.err != nil {
			return b.err
		}
	}
	return nil
}

// Return the type for a type signature.
func (d *Data) sigToType(sig uint64) (Type, error) {
	tu := d.typeSigs[sig]
	if tu == nil {
		return nil, fmt.Errorf("no type unit with signature %v", sig)
	}
	if tu.cache != nil {
		return tu.cache, nil
	}

	b := makeBuf(d, tu, tu.name, tu.off, tu.data)
	r := &typeUnitReader{d: d, tu: tu, b: b}
	t, err := d.readType(tu.name, r, Offset(tu.toff), make(map[Offset]Type))
	if err != nil {
		return nil, err
	}

	tu.cache = t
	return t, nil
}

// typeUnitReader is a typeReader for a tagTypeUnit.
type typeUnitReader struct {
	d   *Data
	tu  *typeUnit
	b   buf
	err error
}

// Seek to a new position in the type unit.
func (tur *typeUnitReader) Seek(off Offset) {
	tur.err = nil
	doff := off - tur.tu.off
	if doff < 0 || doff >= Offset(len(tur.tu.data)) {
		tur.err = fmt.Errorf("%s: offset %d out of range; max %d", tur.tu.name, doff, len(tur.tu.data))
		return
	}
	tur.b = makeBuf(tur.d, tur.tu, tur.tu.name, off, tur.tu.data[doff:])
}

// AddressSize returns the size in bytes of addresses in the current type unit.
func (tur *typeUnitReader) AddressSize() int {
	return tur.tu.unit.asize
}

// Next reads the next Entry from the type unit.
func (tur *typeUnitReader) Next() (*Entry, error) {
	if tur.err != nil {
		return nil, tur.err
	}
	if len(tur.tu.data) == 0 {
		return nil, nil
	}
	e := tur.b.entry(tur.tu.atable, tur.tu.base)
	if tur.b.err != nil {
		tur.err = tur.b.err
		return nil, tur.err
	}
	return e, nil
}

// clone returns a new reader for the type unit.
func (tur *typeUnitReader) clone() typeReader {
	return &typeUnitReader{
		d:  tur.d,
		tu: tur.tu,
		b:  makeBuf(tur.d, tur.tu, tur.tu.name, tur.tu.off, tur.tu.data),
	}
}

// offset returns the current offset.
func (tur *typeUnitReader) offset() Offset {
	return tur.b.off
}
