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

package geometry

import (
	"encoding/binary"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
)

// ParseEWKBHeader converts the header potion of a EWKB byte array to srid, endianness, and geometry type
func ParseEWKBHeader(buf []byte) (srid uint32, bigEndian bool, typ uint32) {
	srid = binary.LittleEndian.Uint32(buf[0:SRIDSize])                          // First 4 bytes is SRID always in little endian
	bigEndian = buf[SRIDSize] == 0                                              // Next byte is endianness
	typ = binary.LittleEndian.Uint32(buf[SRIDSize+EndianSize : EWKBHeaderSize]) // Next 4 bytes is type
	return
}

func ParseEWKBPoint(buf []byte) (x, y float64) {
	x = math.Float64frombits(binary.LittleEndian.Uint64(buf[:PointSize/2]))
	y = math.Float64frombits(binary.LittleEndian.Uint64(buf[PointSize/2:]))
	return
}

func DeserializePoint(buf []byte, srid uint32) (p sql.Point) {
	p.SRID = srid
	p.X, p.Y = ParseEWKBPoint(buf)
	return
}

func DeserializeLinestring(buf []byte, srid uint32) (l sql.Linestring) {
	l.SRID = srid
	l.Points = readPointSlice(buf, srid)
	return
}

func DeserializePolygon(srid uint32, buf []byte) (p sql.Polygon) {
	p.SRID = srid
	p.Lines = readLineSlice(buf, srid)
	return
}

func readCount(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

func readPointSlice(buf []byte, srid uint32) (points []sql.Point) {
	points = make([]sql.Point, readCount(buf))
	buf = buf[CountSize:]
	for i := range points {
		points[i].SRID = srid
		points[i].X, points[i].Y = ParseEWKBPoint(buf)
		buf = buf[PointSize:]
	}
	return
}

func readLineSlice(buf []byte, srid uint32) (lines []sql.Linestring) {
	lines = make([]sql.Linestring, readCount(buf))
	buf = buf[CountSize:]
	for i := range lines {
		lines[i].SRID = srid
		lines[i].Points = readPointSlice(buf, srid)
		sz := len(lines[i].Points) * PointSize
		buf = buf[sz:]
	}
	return
}
