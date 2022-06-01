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

const (
	SRIDSize       = 4
	EndianSize     = 1
	TypeSize       = 4
	EWKBHeaderSize = SRIDSize + EndianSize + TypeSize

	PointSize = 16
	CountSize = 4
)

const (
	PointType      = 1
	LineStringType = 2
	PolygonType    = 3
)

func allocateBuffer(numPoints, numCounts int) []byte {
	return make([]byte, EWKBHeaderSize+PointSize*numPoints+CountSize*numCounts)
}

func WriteEWKBHeader(buf []byte, srid, typ uint32) {
	binary.LittleEndian.PutUint32(buf[0:SRIDSize], srid)
	buf[SRIDSize] = 1
	binary.LittleEndian.PutUint32(buf[SRIDSize+EndianSize:EWKBHeaderSize], typ)
}

func WriteEWKBPointData(buf []byte, x, y float64) {
	binary.LittleEndian.PutUint64(buf[:PointSize/2], math.Float64bits(x))
	binary.LittleEndian.PutUint64(buf[PointSize/2:], math.Float64bits(y))
}

func SerializePoint(p sql.Point) (buf []byte) {
	buf = allocateBuffer(1, 0)
	WriteEWKBHeader(buf[:EWKBHeaderSize], p.SRID, PointType)
	WriteEWKBPointData(buf[EWKBHeaderSize:], p.X, p.Y)
	return
}

func SerializeLineString(l sql.LineString) (buf []byte) {
	buf = allocateBuffer(len(l.Points), 1)
	WriteEWKBHeader(buf[:EWKBHeaderSize], l.SRID, LineStringType)
	writePointSlice(buf[EWKBHeaderSize:], l.Points)
	return
}

func SerializePolygon(p sql.Polygon) (buf []byte) {
	buf = allocateBuffer(countPoints(p), len(p.Lines)+1)
	WriteEWKBHeader(buf[:EWKBHeaderSize], p.SRID, PolygonType)
	writeLineSlice(buf[EWKBHeaderSize:], p.Lines)
	return
}

func writeCount(buf []byte, count uint32) {
	binary.LittleEndian.PutUint32(buf, count)
}

func writePointSlice(buf []byte, points []sql.Point) {
	writeCount(buf, uint32(len(points)))
	buf = buf[CountSize:]
	for _, p := range points {
		WriteEWKBPointData(buf, p.X, p.Y)
		buf = buf[PointSize:]
	}
}

func writeLineSlice(buf []byte, lines []sql.LineString) {
	writeCount(buf, uint32(len(lines)))
	buf = buf[CountSize:]
	for _, l := range lines {
		writePointSlice(buf, l.Points)
		sz := len(l.Points) * PointSize
		buf = buf[sz:]
	}
}

func countPoints(p sql.Polygon) (cnt int) {
	for _, line := range p.Lines {
		cnt += len(line.Points)
	}
	return
}
