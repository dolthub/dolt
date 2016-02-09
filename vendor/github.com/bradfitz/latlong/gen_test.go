// +build latlong_gen

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

package latlong

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"go/format"
	"hash/crc32"
	"image"
	"image/color"
	"image/png"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"testing"
	"time"

	"code.google.com/p/freetype-go/freetype/raster"
	"github.com/jonas-p/go-shp"
)

var (
	flagGenerate   = flag.Bool("generate", false, "Do generation")
	flagWriteImage = flag.Bool("write_image", false, "Write out a debug image")
	flagScale      = flag.Float64("scale", 32, "Scaling factor. This many pixels wide & tall per degree (e.g. scale 1 is 360 x 180). Increasingly this code assumes a scale of 32, though.")
)

func saveToPNGFile(filePath string, m image.Image) {
	log.Printf("Encoding image %s ...", filePath)
	f, err := os.Create(filePath)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer f.Close()
	b := bufio.NewWriter(f)
	err = png.Encode(b, m)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	err = b.Flush()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Wrote %s OK.\n", filePath)
}

func cloneImage(i *image.RGBA) *image.RGBA {
	i2 := new(image.RGBA)
	*i2 = *i
	i2.Pix = make([]uint8, len(i.Pix))
	copy(i2.Pix, i.Pix)
	return i2
}

func loadImage(filename string) *image.NRGBA {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	im, _, err := image.Decode(f)
	if err != nil {
		log.Fatal(err)
	}
	return im.(*image.NRGBA)
}

const alphaErased = 22 // magic alpha value to mean tile's been erased

// the returned zoneOfColor always has A == 256.
func worldImage(t *testing.T) (im *image.RGBA, zoneOfColor map[color.RGBA]string) {
	scale := *flagScale
	width := int(scale * 360)
	height := int(scale * 180)

	im = image.NewRGBA(image.Rect(0, 0, width, height))
	zoneOfColor = map[color.RGBA]string{}
	tab := crc32.MakeTable(crc32.IEEE + 1)

	drawPoly := func(col color.RGBA, xys ...int) {
		painter := raster.NewRGBAPainter(im)
		painter.SetColor(col)
		r := raster.NewRasterizer(width, height)
		r.Start(raster.Point{X: raster.Fix32(xys[0]) << 8, Y: raster.Fix32(xys[1]) << 8})
		for i := 2; i < len(xys); i += 2 {
			r.Add1(raster.Point{X: raster.Fix32(xys[i]) << 8, Y: raster.Fix32(xys[i+1]) << 8})
		}
		r.Add1(raster.Point{X: raster.Fix32(xys[0]) << 8, Y: raster.Fix32(xys[1]) << 8})
		r.Rasterize(raster.NewMonochromePainter(painter))
	}

	sr, err := shp.Open("world/tz_world.shp")
	if err != nil {
		t.Fatalf("Error opening world/tz_world.shp: %v; unzip it from http://efele.net/maps/tz/world/tz_world.zip", err)
	}
	defer sr.Close()

	for sr.Next() {
		i, s := sr.Shape()
		p, ok := s.(*shp.Polygon)
		if !ok {
			t.Fatalf("Unknown shape %T", p)
		}
		zoneName := sr.ReadAttribute(i, 0)
		if zoneName == "uninhabited" {
			continue
		}
		if _, err := time.LoadLocation(zoneName); err != nil {
			t.Fatalf("Failed to load: %v (%v)", zoneName, err)
		}
		hash := crc32.Checksum([]byte(zoneName), tab)
		col := color.RGBA{uint8(hash >> 24), uint8(hash >> 16), uint8(hash >> 8), 255}
		if name, ok := zoneOfColor[col]; ok {
			if name != zoneName {
				log.Fatalf("Color %+v dup: %s and %s", col, name, zoneName)
			}
		} else {
			zoneOfColor[col] = zoneName
		}

		var xys []int
		for _, pt := range p.Points {
			xys = append(xys, int((pt.X+180)*scale), int((90-pt.Y)*scale))
		}
		drawPoly(col, xys...)
	}

	// adjust point from scale 32 to whatever the user is using.
	ap := func(x int) int { return x * int(scale) / 32 }
	// Fix some rendering glitches:
	// {186 205 234 255} = Europe/Rome
	drawPoly(color.RGBA{186, 205, 234, 255},
		ap(6156), ap(1468),
		ap(6293), ap(1596),
		ap(6293), ap(1598),
		ap(6156), ap(1540))
	// {136 136 180 255} = America/Boise
	drawPoly(color.RGBA{136, 136, 180, 255},
		ap(2145), ap(1468),
		ap(2189), ap(1468),
		ap(2189), ap(1536),
		ap(2145), ap(1536))
	// {120 247 14 255} = America/Denver
	drawPoly(color.RGBA{120, 247, 14, 255},
		ap(2167), ap(1536),
		ap(2171), ap(1536),
		ap(2217), ap(1714),
		ap(2204), ap(1724),
		ap(2160), ap(1537))
	return
}

// A setIndexTracker that tells each index which item number it is, and can
// retrieve that item's index later as well.
type setIndexTracker struct {
	s map[interface{}]uint16
	l []interface{}
}

func (s *setIndexTracker) Lookup(v interface{}) (idx uint16, ok bool) {
	idx, ok = s.s[v]
	return
}

func (s *setIndexTracker) Add(v interface{}) (idx uint16, isNew bool) {
	if idx, ok := s.s[v]; ok {
		return idx, false
	}

	if len(s.s) > 0xffff {
		panic("too many items in set")
	}
	idx = uint16(len(s.s))
	if s.s == nil {
		s.s = make(map[interface{}]uint16)
	}
	s.s[v] = idx
	s.l = append(s.l, v)
	return idx, true
}

func init() {
	testAllPixels = testAllPixels_gen
}

func testAllPixels_gen(t *testing.T) {
	if degPixels == -1 {
		t.Skip("data not generated yet")
	}
	im, zoneOfColor := worldImage(t)
	w := im.Bounds().Max.X
	h := im.Bounds().Max.Y
	total, fail := 0, 0
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			pix := im.Pix[im.PixOffset(x, y):]
			if pix[3] == 0 {
				continue
			}
			total++
			c := color.RGBA{
				R: pix[0],
				G: pix[1],
				B: pix[2],
				A: 255,
			}
			want := zoneOfColor[c]
			if got := lookupPixel(x, y); got != want {
				fail++
				if fail <= 10 {
					t.Errorf("pixel(%d, %d) = %q; want %q", x, y, got, want)
				}
			}
		}
	}
	t.Logf("%d pixels tested; %d failures", total, fail)
}

func TestGenerate(t *testing.T) {
	if !*flagGenerate {
		t.Skip("skipping generationg without --generate flag")
	}

	im, zoneOfColor := worldImage(t)

	// The auto-generated source file (z_gen_tables.go)
	var gen bytes.Buffer
	gen.WriteString("// Auto-generated file. See README or Makefile.\n\npackage latlong\n\n")
	gen.WriteString("func init() {\n")

	fmt.Fprintf(&gen, "degPixels = %d\n", int(*flagScale))

	// Source code for just the zoneLookers variables.
	var zoneLookers zoneLookerWriter

	// Maps from a unique key (either a string or colorTile) to
	// its index.
	var zoneIndex setIndexTracker

	zoneIndexOfColor := func(c color.RGBA) uint16 {
		if (c == color.RGBA{}) {
			return oceanIndex
		}
		idx, ok := zoneIndex.Lookup(zoneOfColor[c])
		if !ok {
			t.Fatalf("failed to find zone index for color %+v", c)
		}
		return idx
	}

	// Add the static timezones (~408 of them). If a tile (which
	// can range from 8 to 256 pixels square) doesn't resolve to
	// one of these, it'll resolve to an image tile that then
	// resolves to one of these.
	{
		var zones []string
		for _, zone := range zoneOfColor {
			zones = append(zones, zone)
		}
		sort.Strings(zones)
		for i, zone := range zones {
			idx, _ := zoneIndex.Add(zone)
			if idx != uint16(i) {
				panic("unexpected")
			}
			zoneLookers.Add("S" + zone)
		}
		log.Printf("Num zones = %d", len(zones))
	}

	var imo *image.RGBA
	if *flagWriteImage {
		imo = cloneImage(im)
	}
	dupColorTiles := 0

	gen.WriteString("zoomLevels = [6]*zoomLevel{\n")
	for _, sizeShift := range []uint8{5, 4, 3, 2, 1, 0} {
		fmt.Fprintf(&gen, "\t%d: &zoomLevel{\n", sizeShift)
		var keyIdxBuf bytes.Buffer // repeated binary [tilekey][uint16_idx]

		pass := newSizePass(im, imo, sizeShift)

		skipSquares := 0
		sizeCount := map[int]int{} // num colors -> count

		pass.foreachTile(func(tile *tileMeta) {
			if tile.skipped {
				skipSquares++
				return
			}
			nColor := len(tile.colors)
			sizeCount[nColor]++
			if nColor < 2 {
				tile.erase()
			}
			if nColor == 1 {
				zoneName := zoneOfColor[tile.color()]
				if idx, isNew := zoneIndex.Add(zoneName); isNew {
					panic("zone should've been registered: " + zoneName)
				} else {
					binary.Write(&keyIdxBuf, binary.BigEndian, tile.key())
					binary.Write(&keyIdxBuf, binary.BigEndian, idx)
				}
				tile.drawBorder()
				return
			}
			if nColor == 0 {
				tile.paintOcean()
				return
			}
			if sizeShift == 0 && nColor >= 2 {
				ct := tile.colorTile()
				idx, isNew := zoneIndex.Add(ct)
				if isNew {
					if nColor == 2 {
						zoneLookers.Add(fmt.Sprintf("2%s", pass.bitmapPixmapBytes(ct, zoneIndexOfColor)))
					} else {
						zoneLookers.Add(fmt.Sprintf("P%s", pass.pixmapIndexBytes(ct, zoneIndexOfColor)))
					}
				} else {
					dupColorTiles++
				}
				binary.Write(&keyIdxBuf, binary.BigEndian, tile.key())
				binary.Write(&keyIdxBuf, binary.BigEndian, idx)
			}
		})
		log.Printf("For size %d, skipped %d, dist: %+v", pass.size, skipSquares, sizeCount)

		var zbuf bytes.Buffer
		zw := gzip.NewWriter(&zbuf)
		zw.Write(keyIdxBuf.Bytes())
		zw.Close()

		log.Printf("size %d is %d entries: %d bytes (%d bytes compressed)", pass.size, keyIdxBuf.Len()/6, keyIdxBuf.Len(), zbuf.Len())

		fmt.Fprintf(&gen, "\t\tgzipData: %q,\n", base64.StdEncoding.EncodeToString(zbuf.Bytes()))
		gen.WriteString("\t},\n")
	}
	gen.WriteString("}\n\n")

	log.Printf("Duplicate 8x8 pixmaps: %d", dupColorTiles)

	if imo != nil {
		saveToPNGFile("regions.png", imo)
	}

	gen.Write(zoneLookers.Source())
	gen.WriteString("}\n") // close init

	fmt, err := format.Source(gen.Bytes())
	if err != nil {
		ioutil.WriteFile("z_gen_tables.go", gen.Bytes(), 0644)
		t.Fatal(err)
	}
	if err := ioutil.WriteFile("z_gen_tables.go", fmt, 0644); err != nil {
		t.Fatal(err)
	}
}

type sizePass struct {
	width, height  int
	size           int // of tile. 8 << sizeShift
	sizeShift      uint8
	xtiles, ytiles int
	im             *image.RGBA
	imo            *image.RGBA // or nil if not generating an output image

	buf [128]byte // for pixmap 8x8 uint16 indexes
}

func newSizePass(im, imo *image.RGBA, sizeShift uint8) *sizePass {
	p := &sizePass{
		width:     im.Bounds().Max.X,
		height:    im.Bounds().Max.Y,
		im:        im,
		imo:       imo,
		sizeShift: sizeShift,
		size:      int(8 << sizeShift),
	}
	p.xtiles = p.width / p.size
	p.ytiles = p.height / p.size
	return p
}

func (p *sizePass) foreachTile(fn func(*tileMeta)) {
	tm := new(tileMeta)
	im := p.im

	colors := map[color.RGBA]bool{}
	// clearColors wipes colors, so we can re-use it.
	clearColors := func() {
		for k := range colors {
			delete(colors, k)
		}
	}

	for yt := 0; yt < p.ytiles; yt++ {
		for xt := 0; xt < p.xtiles; xt++ {
			*tm = tileMeta{p: p, xt: xt, yt: yt, colors: colors}
			tm.setBounds()
			sawOcean := false
			x1, y1 := tm.x1, tm.y1
			clearColors()
		Pixels:
			for y := tm.y0; y < y1; y++ {
				for x := tm.x0; x < x1; x++ {
					off := im.PixOffset(x, y)
					alpha := im.Pix[off+3]
					switch alpha {
					case 0:
						sawOcean = true
						continue
					case alphaErased:
						if x != tm.x0 || y != tm.y0 {
							panic("unexpected")
						}
						tm.skipped = true
						break Pixels
					case 255:
						// expected
					default:
						panic("Unexpected alpha value")
					}
					nc := color.RGBA{R: im.Pix[off], G: im.Pix[off+1], B: im.Pix[off+2], A: alpha}
					colors[nc] = true
				}
			}
			if len(colors) > 1 && sawOcean {
				// note the ocean, since this can't be solid anyway
				colors[color.RGBA{}] = true
			}
			fn(tm)
		}
	}
}

func (p *sizePass) pixmapIndexBytes(ct colorTile, fn func(color.RGBA) uint16) []byte {
	buf := p.buf[:]
	for _, row := range ct {
		for _, c := range row {
			binary.BigEndian.PutUint16(buf, fn(c))
			buf = buf[2:]
		}
	}
	return p.buf[:128]
}

// For two-color tiles.
func (p *sizePass) bitmapPixmapBytes(ct colorTile, fn func(color.RGBA) uint16) []byte {
	var c1, c2 color.RGBA
	var bits uint64
	var n uint8
	for _, row := range ct {
		for _, c := range row {
			if n == 0 {
				c1 = c
			} else {
				if c != c1 {
					c2 = c
					bits |= (1 << n)
				}
			}
			n++
		}
	}
	if c1 == c2 {
		panic("didn't see two colors")
	}
	binary.BigEndian.PutUint16(p.buf[0:2], fn(c1))
	binary.BigEndian.PutUint16(p.buf[2:4], fn(c2))
	binary.BigEndian.PutUint64(p.buf[4:12], bits)
	return p.buf[:12]
}

type tileMeta struct {
	p      *sizePass
	xt, yt int

	x0, x1, y0, y1 int

	// skipped reports whether the tile was skipped due to seeing
	// erasure from previous level.
	skipped bool

	// colors will only contain a zero color if the tile size is smallest
	// (8x8) and there's an ocean (zero color) and two others. If there's
	// an ocean and only 1 other color, only one color will be returned.
	colors map[color.RGBA]bool
}

func (t *tileMeta) setBounds() {
	size := t.p.size
	t.y0 = t.yt * size
	t.y1 = (t.yt + 1) * size
	t.x0 = t.xt * size
	t.x1 = (t.xt + 1) * size
}

func (t *tileMeta) color() color.RGBA {
	if len(t.colors) != 1 {
		panic("color called with colors != 1")
	}
	var c color.RGBA
	for c = range t.colors {
		// get first (and only) key
	}
	if (c == color.RGBA{}) {
		panic("no color found for tile")
	}
	return c
}

func (t *tileMeta) key() tileKey {
	return newTileKey(t.p.sizeShift, uint16(t.xt), uint16(t.yt))
}

func (t *tileMeta) paintOcean() {
	p := t.p
	imo := p.imo
	if imo == nil {
		return
	}
	blue := [4]uint8{0, 0, 128, 255}
	size := p.size
	for y := t.y0; y < t.y1; y++ {
		off := imo.PixOffset(t.x0, y)
		for x := 0; x < size; x++ {
			copy(imo.Pix[off:], blue[:])
			off += 4
		}
	}
}

func (t *tileMeta) drawBorder() {
	p := t.p
	imo := p.imo
	if imo == nil {
		return
	}

	yellow := uint8(255 - (128 - byte(p.size)))
	color := [4]uint8{yellow, yellow, 0, 255}

	for y := t.y0; y < t.y1; y++ {
		off := imo.PixOffset(t.x0, y)
		for x := t.x0; x < t.x1; x++ {
			// Border:
			if y == t.y0 || y == t.y1-1 || x == t.x0 || x == t.x1-1 {
				copy(imo.Pix[off:], color[:])
			}
			off += 4
		}
	}
}

func (t *tileMeta) erase() {
	im := t.p.im
	for y := t.y0; y < t.y1; y += 8 {
		for x := t.x0; x < t.x1; x += 8 {
			off := im.PixOffset(x, y)
			im.Pix[off+3] = alphaErased
		}
	}

}

func (t *tileMeta) colorTile() (ct colorTile) {
	im := t.p.im
	for y := range ct {
		row := &ct[y]
		pix := im.Pix[im.PixOffset(t.x0, t.y0+y):]
		for x := range row {
			row[x] = color.RGBA{pix[0], pix[1], pix[2], pix[3]}
			pix = pix[4:]
		}
	}
	return
}

type colorTile [8][8]color.RGBA

type zoneLookerWriter struct {
	unbuf bytes.Buffer
	n     int
}

func (w *zoneLookerWriter) Add(s string) {
	w.n++
	if w.n > 0xffff {
		panic("too many unique leaves")
	}
	w.unbuf.WriteString(s)
	switch s[0] {
	case 'S':
		w.unbuf.WriteByte(0)
	case '2':
		if len(s) != 12+1 {
			panic("unexpected length")
		}
	case 'P':
		if len(s) != 128+1 {
			panic("unexpected length")
		}
	default:
		panic("unexpected type")
	}
}

func (w *zoneLookerWriter) Source() []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(w.unbuf.Bytes())
	zw.Close()

	bstr := base64.StdEncoding.EncodeToString(buf.Bytes())
	buf.Reset()
	fmt.Fprintf(&buf, "leaf = make([]zoneLooker, %d)\n", w.n)
	fmt.Fprintf(&buf, "uniqueLeavesPacked = %q\n", bstr)
	log.Printf("zone lookers packed line = %d bytes", buf.Len())
	return buf.Bytes()
}
