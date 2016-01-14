/*
Copyright 2014 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package latlong maps from a latitude and longitude to a timezone.
//
// It uses the data from http://efele.net/maps/tz/world/ compressed down
// to an internal form optimized for low memory overhead and fast lookups
// at the expense of perfect accuracy when close to borders. The data files
// are compiled in to this package and do not require explicit loading.
package latlong

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
)

// Populated by z_gen_tables.go:
var (
	degPixels          = -1
	zoomLevels         [6]*zoomLevel
	uniqueLeavesPacked string
	leaf               []zoneLooker
)

// LookupZoneName returns the timezone name at the given latitude and
// longitude. The returned name is either the empty string (if not
// found) or a name suitable for passing to time.LoadLocation. For
// example, "America/New_York".
func LookupZoneName(lat, long float64) string {
	x := int((long + 180) * float64(degPixels))
	y := int((90 - lat) * float64(degPixels))
	if x < 0 {
		x = 0
	} else if x >= 360*degPixels {
		x = 360*degPixels - 1
	}
	if y < 0 {
		y = 0
	} else if y >= 180*degPixels {
		y = 180*degPixels - 1
	}
	return lookupPixel(x, y)
}

func lookupPixel(x, y int) string {
	if degPixels == -1 {
		return "tables not generated yet"
	}
	unpackOnce.Do(unpackTables)

	for level := 5; level >= 0; level-- {
		shift := 3 + uint8(level)
		xt := uint16(x >> shift)
		yt := uint16(y >> shift)
		tk := newTileKey(uint8(level), xt, yt)
		zone, ok := zoomLevels[level].LookupZone(x, y, tk)
		if ok {
			return zone
		}
	}
	return ""
}

var unpackOnce sync.Once

func unpackTables() {
	for _, zl := range zoomLevels {
		zr, err := gzip.NewReader(
			base64.NewDecoder(base64.StdEncoding,
				strings.NewReader(zl.gzipData)))
		check(err)
		slurp, err := ioutil.ReadAll(zr)
		check(err)
		if len(slurp)%6 != 0 {
			panic("bogus encoded tileLooker length")
		}
		zl.tiles = make([]tileLooker, len(slurp)/6)
		for i := range zl.tiles {
			idx := i * 6
			zl.tiles[i] = tileLooker{
				tileKey(binary.BigEndian.Uint32(slurp[idx : idx+4])),
				binary.BigEndian.Uint16(slurp[idx+4 : idx+6]),
			}
		}
	}

	zr, err := gzip.NewReader(
		base64.NewDecoder(base64.StdEncoding,
			strings.NewReader(uniqueLeavesPacked)))
	check(err)
	br := bufio.NewReader(zr)
	var buf [128]byte
	for i := range leaf {
		t, err := br.ReadByte()
		check(err)
		switch t {
		default:
			panic("unknown leaf type: " + fmt.Sprintf("%q", t))
		case 'S': // static zone
			v, err := br.ReadBytes(0) // null-terminated
			check(err)
			leaf[i] = staticZone(string(v[:len(v)-1]))
		case '2': // two-timezone 1bpp bitmap (pass.bitmapPixmapBytes)
			_, err := io.ReadFull(br, buf[:12])
			check(err)
			t := oneBitTile{
				idx: [2]uint16{
					binary.BigEndian.Uint16(buf[0:2]),
					binary.BigEndian.Uint16(buf[2:4]),
				},
			}
			bits := binary.BigEndian.Uint64(buf[4:12])
			for y := range t.rows {
				for x := 0; x < 8; x++ {
					if bits&(1<<uint(y*8+x)) != 0 {
						t.rows[y] |= (1 << uint(x))
					}
				}
			}
			leaf[i] = t
		case 'P': // multi-timezone 4bpp bitmap
			_, err := io.ReadFull(br, buf[:128])
			check(err)
			leaf[i] = pixmap(buf[:128])
		}
	}

}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type zoneLooker interface {
	LookupZone(x, y int, tk tileKey) (zone string, ok bool)
}

type staticZone string

func (z staticZone) LookupZone(x, y int, tk tileKey) (zone string, ok bool) {
	return string(z), true
}

// A tilekey is a packed 32 bit integer where:
// 3 high bits: tile size: 8<<n (8 to 256 for n=0-5)
// bits 0-13 bits: x tile position
// bits 14-27 bits: y tile position
// bit 28: unused
// bit 31,30,29: tile size
// ssss
type tileKey uint32

// size is 0, 1, 2, or 3
func newTileKey(size uint8, x, y uint16) tileKey {
	return tileKey(size&7)<<28 |
		tileKey(y&(1<<14-1))<<14 |
		tileKey(x&(1<<14-1))
}

func (v tileKey) size() uint8 {
	return byte(v >> 28)
}

func (v tileKey) x() uint16 {
	return uint16(v & (1<<14 - 1))
}

func (v tileKey) y() uint16 {
	return uint16((v >> 14) & (1<<14 - 1))
}

type tileLooker struct {
	tile tileKey
	idx  uint16 // index into leaf
}

type zoomLevel struct {
	gzipData string       // compressed [tilekey][uint16_idx], repeated
	tiles    []tileLooker // lazily populated
}

func (zl *zoomLevel) LookupZone(x, y int, tk tileKey) (zone string, ok bool) {
	pos := sort.Search(len(zl.tiles), func(i int) bool {
		return zl.tiles[i].tile >= tk
	})
	if pos >= len(zl.tiles) {
		return
	}
	tl := zl.tiles[pos]
	if tl.tile != tk {
		return
	}
	return leaf[tl.idx].LookupZone(x, y, tk)
}

// A oneBitTile represents a fully opaque 8x8 grid tile that only has
// two colors. The idx represents the indexes of the two colors (the palette)
// table, and the rows are the bits.
type oneBitTile struct {
	idx  [2]uint16 // bit 0 and bit 1's index into []leaf
	rows [8]uint8  // [y], then 1<<x.
}

func (t oneBitTile) LookupZone(x, y int, tk tileKey) (zone string, ok bool) {
	idx := t.idx[0]
	if t.rows[y&7]&(1<<(uint(x&7))) != 0 {
		idx = t.idx[1]
	}
	return leaf[idx].LookupZone(x, y, tk)
}

// pixmap packs 8x8 row-order big ending uint16 indexes into
// zoneLookers. Each string is 128 bytes long.
type pixmap string

func (p pixmap) LookupZone(x, y int, tk tileKey) (zone string, ok bool) {
	xx := x & 7
	yy := y & 7
	i := 2 * (yy*8 + xx)
	idx := uint16(p[i])<<8 + uint16(p[i+1])
	if idx == oceanIndex {
		return "", true
	}
	return leaf[idx].LookupZone(x, y, tk)
}

// The oceanIndex is a magic index into zoneLooker which says that
// it's invalid and there's an ocean or something there. Unknown
// timezone.
const oceanIndex uint16 = 0xffff
