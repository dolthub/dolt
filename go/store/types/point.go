// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	SRIDSize = 4
	EndianSize = 1
	TypeSize = 4
	EWKBHeaderSize = SRIDSize + EndianSize + TypeSize
	PointDataSize = 16
	PointID = 1
	LinestringID = 2
	PolygonID = 3
)

// Point is a Noms Value wrapper around the primitive string type (for now).
type Point struct {
	SRID uint32
	X    float64
	Y    float64
}

// Value interface
func (v Point) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Point) Equals(other Value) bool {
	if v2, ok := other.(Point); ok {
		return v.SRID == v2.SRID && v.X == v2.X && v.Y == v2.Y
	}
	return false
}

func (v Point) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if v2, ok := other.(Point); ok {
		return v.SRID < v2.SRID || v.X < v2.X || v.Y < v2.Y, nil
	}
	return PointKind < other.Kind(), nil
}

func (v Point) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Point) isPrimitive() bool {
	return true
}

func (v Point) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Point) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Point) typeOf() (*Type, error) {
	return PrimitiveTypeMap[PointKind], nil
}

func (v Point) Kind() NomsKind {
	return PointKind
}

func (v Point) valueReadWriter() ValueReadWriter {
	return nil
}

// writeEWKBHeader writes the SRID, endianness, and type to the byte buffer
// This function assumes v is a valid spatial type
func writeEWKBHeader(v interface{}, buf []byte) {
	// Write endianness byte (always little endian)
	buf[4] = 1

	// Parse data
	switch v := v.(type) {
	case Point:
		// Write SRID and type
		binary.LittleEndian.PutUint32(buf[0:SRIDSize], v.SRID)
		binary.LittleEndian.PutUint32(buf[SRIDSize + EndianSize:EWKBHeaderSize], PointID)
	case Linestring:
		binary.LittleEndian.PutUint32(buf[0:SRIDSize], v.SRID)
		binary.LittleEndian.PutUint32(buf[SRIDSize + EndianSize:EWKBHeaderSize], LinestringID)
	case Polygon:
		binary.LittleEndian.PutUint32(buf[0:SRIDSize], v.SRID)
		binary.LittleEndian.PutUint32(buf[SRIDSize + EndianSize:EWKBHeaderSize], PolygonID)
	}
}

// writeEWKBPointData converts a Point into a byte array in EWKB format
// Very similar to function in GMS
func writeEWKBPointData(p Point, buf []byte) {
	binary.LittleEndian.PutUint64(buf[:PointDataSize/2], math.Float64bits(p.X))
	binary.LittleEndian.PutUint64(buf[PointDataSize/2:], math.Float64bits(p.Y))
}

func (v Point) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	// Mark as PointKind
	err := PointKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	// Allocate buffer for point 4 + 1 + 4 + 16
	buf := make([]byte, EWKBHeaderSize + PointDataSize)

	// Write header and data to buffer
	writeEWKBHeader(v, buf)
	writeEWKBPointData(v, buf[EWKBHeaderSize:])

	w.writeString(string(buf))
	return nil
}

// parseEWKBHeader converts the header potion of a EWKB byte array to srid, endianness, and geometry type
func parseEWKBHeader(buf []byte) (uint32, bool, uint32) {
	srid := binary.LittleEndian.Uint32(buf[0:SRIDSize])     // First 4 bytes is SRID always in little endian
	isBig := buf[SRIDSize] == 0                             // Next byte is endianness
	geomType := binary.LittleEndian.Uint32(buf[SRIDSize + EndianSize:EWKBHeaderSize]) // Next 4 bytes is type
	return srid, isBig, geomType
}

// parseEWKBPoint converts the data portion of a WKB point to Point
// Very similar logic to the function in GMS
func parseEWKBPoint(buf []byte, srid uint32) Point {
	// Read floats x and y
	x := math.Float64frombits(binary.LittleEndian.Uint64(buf[:PointDataSize/2]))
	y := math.Float64frombits(binary.LittleEndian.Uint64(buf[PointDataSize/2:]))
	return Point{SRID: srid, X: x, Y: y}
}

func readPoint(nbf *NomsBinFormat, b *valueDecoder) (Point, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := parseEWKBHeader(buf) // Assume it's always little endian
	if geomType != 1 {
		return Point{}, errors.New("not a point")
	}
	return parseEWKBPoint(buf[EWKBHeaderSize:], srid), nil
}

func (v Point) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := parseEWKBHeader(buf) // Assume it's always little endian
	if geomType != 1 {
		return Point{}, errors.New("not a point")
	}
	return parseEWKBPoint(buf[EWKBHeaderSize:], srid), nil
}

func (v Point) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Point) HumanReadableString() string {
	s := fmt.Sprintf("SRID: %d POINT(%s %s)", v.SRID, strconv.FormatFloat(v.X, 'g', -1, 64), strconv.FormatFloat(v.Y, 'g', -1, 64))
	return strconv.Quote(s)
}
