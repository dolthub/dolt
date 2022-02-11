// Copyright 2021 Dolthub, Inc.
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

package types

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/store/geometry"

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	LengthSize = 4
)

// Linestring is a Noms Value wrapper around a string.
type Linestring struct {
	SRID   uint32
	Points []Point
}

// Value interface
func (v Linestring) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Linestring) Equals(other Value) bool {
	// Compare types
	v2, ok := other.(Linestring)
	if !ok {
		return false
	}
	// Compare SRID
	if v.SRID != v2.SRID {
		return false
	}
	// Compare lengths of points
	if len(v.Points) != len(v2.Points) {
		return false
	}
	// Compare each point
	for i := 0; i < len(v.Points); i++ {
		if !v.Points[i].Equals(v2.Points[i]) {
			return false
		}
	}
	return true
}

func (v Linestring) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	// Compare types
	v2, ok := other.(Linestring)
	if !ok {
		return LinestringKind < other.Kind(), nil
	}
	// TODO: should I even take this into account?
	// Compare SRID
	if v.SRID != v2.SRID {
		return v.SRID < v2.SRID, nil
	}
	// Get shorter length
	var n int
	len1 := len(v.Points)
	len2 := len(v2.Points)
	if len1 < len2 {
		n = len1
	} else {
		n = len2
	}

	// Compare each point until there's one that is less than
	for i := 0; i < n; i++ {
		if !v.Points[i].Equals(v2.Points[i]) {
			return v.Points[i].Less(nbf, v2.Points[i])
		}
	}

	// Determine based off length
	return len1 < len2, nil
}

func (v Linestring) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Linestring) isPrimitive() bool {
	return true
}

func (v Linestring) WalkValues(ctx context.Context, cb ValueCallback) error {
	for _, p := range v.Points {
		if err := p.WalkValues(ctx, cb); err != nil {
			return err
		}
	}
	return nil
}

func (v Linestring) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Linestring) typeOf() (*Type, error) {
	return PrimitiveTypeMap[LinestringKind], nil
}

func (v Linestring) Kind() NomsKind {
	return LinestringKind
}

func (v Linestring) valueReadWriter() ValueReadWriter {
	return nil
}

// WriteEWKBLineData converts a Line into a byte array in EWKB format
func WriteEWKBLineData(l Linestring, buf []byte) {
	// Write length of linestring
	binary.LittleEndian.PutUint32(buf[:LengthSize], uint32(len(l.Points)))
	// Append each point
	for i, p := range l.Points {
		WriteEWKBPointData(p, buf[LengthSize+geometry.PointSize*i:LengthSize+geometry.PointSize*(i+1)])
	}
}

func (v Linestring) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := LinestringKind.writeTo(w, nbf)
	if err != nil {
		return err
	}

	// Allocate buffer for linestring
	buf := make([]byte, geometry.EWKBHeaderSize+LengthSize+geometry.PointSize*len(v.Points))

	// Write header and data to buffer
	WriteEWKBHeader(v, buf)
	WriteEWKBLineData(v, buf[geometry.EWKBHeaderSize:])

	w.writeString(string(buf))
	return nil
}

// ParseEWKBLine converts the data portion of a WKB point to Linestring
// Very similar logic to the function in GMS
func ParseEWKBLine(buf []byte, srid uint32) Linestring {
	// Read length of linestring
	numPoints := binary.LittleEndian.Uint32(buf[:4])

	// Parse points
	points := make([]Point, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		points[i] = ParseEWKBPoint(buf[LengthSize+geometry.PointSize*i:LengthSize+geometry.PointSize*(i+1)], srid)
	}

	return Linestring{SRID: srid, Points: points}
}

func readLinestring(nbf *NomsBinFormat, b *valueDecoder) (Linestring, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf)
	if geomType != geometry.LinestringType {
		return Linestring{}, errors.New("not a linestring")
	}
	return ParseEWKBLine(buf[geometry.EWKBHeaderSize:], srid), nil
}

func (v Linestring) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	buf := []byte(b.ReadString())
	srid, _, geomType := geometry.ParseEWKBHeader(buf)
	if geomType != geometry.LinestringType {
		return nil, errors.New("not a linestring")
	}
	return ParseEWKBLine(buf[geometry.EWKBHeaderSize:], srid), nil
}

func (v Linestring) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	b.skipString()
}

func (v Linestring) HumanReadableString() string {
	points := make([]string, len(v.Points))
	for i, p := range v.Points {
		points[i] = p.HumanReadableString()
	}
	s := fmt.Sprintf("SRID: %d LINESTRING(%s)", v.SRID, strings.Join(points, ","))
	return strconv.Quote(s)
}
