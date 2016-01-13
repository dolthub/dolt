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

import "testing"

func TestLookupLatLong(t *testing.T) {
	cases := []struct {
		lat, long float64
		want      string
	}{
		{37.7833, -122.4167, "America/Los_Angeles"},
	}
	for _, tt := range cases {
		if got := LookupZoneName(tt.lat, tt.long); got != tt.want {
			t.Errorf("LookupZoneName(%v, %v) = %q; want %q", tt.lat, tt.long, got, tt.want)
		}
	}
}

var testAllPixels func(t *testing.T)

func TestAllPixels(t *testing.T) {
	if testAllPixels == nil {
		t.Skip("exhaustive pixel test disabled without --tags=latlong_gen (requires extra deps)")
	}
	testAllPixels(t)
}

func TestLookupPixel(t *testing.T) {
	cases := []struct {
		x, y int
		want string
	}{
		// previous bug with the wrong oceanIndex. This pixmap
		// leaf resolves to leaf index 0xff (Asia/Phnom_Penh).
		// It was being treated as a special value. It's not.
		{9200, 2410, "Asia/Phnom_Penh"},
		{9047, 2488, "Asia/Phnom_Penh"},

		// one-bit leaf tile:
		{9290, 530, "Asia/Krasnoyarsk"},
		{9290, 531, "Asia/Yakutsk"},

		// four-bit tile:
		{2985, 1654, "America/Indiana/Vincennes"},
		{2986, 1654, "America/Indiana/Marengo"},
		{2986, 1655, "America/Indiana/Tell_City"},

		// Empty tile:
		{4000, 2000, ""},

		// Big 1-color tile in ocean with island:
		{3687, 1845, "Atlantic/Bermuda"},
		// Same, but off Oregon coast:
		{1747, 1486, "America/Los_Angeles"},

		// Little solid tile:
		{2924, 2316, "America/Belize"},
	}
	for _, tt := range cases {
		if got := lookupPixel(tt.x, tt.y); got != tt.want {
			t.Errorf("lookupPixel(%v, %v) = %q; want %q", tt.x, tt.y, got, tt.want)
		}
	}
}

func TestNewTileKey(t *testing.T) {
	cases := []struct {
		size, x, y int
	}{
		{0, 1<<14 - 1, 1<<14 - 1},
		{0, 1<<14 - 1, 0},
		{0, 0, 1<<14 - 1},
		{0, 0, 0},
		{1, 1, 1},
		{1, 2, 3},
		{2, 3, 1},
		{3, 3, 3},
	}
	for i, tt := range cases {
		tk := newTileKey(byte(tt.size), uint16(tt.x), uint16(tt.y))
		if tk.size() != byte(tt.size) {
			t.Errorf("%d. size = %d; want %d", i, tk.size(), tt.size)
		}
		if tk.x() != uint16(tt.x) {
			t.Errorf("%d. x = %d; want %d", i, tk.x(), tt.x)
		}
		if tk.y() != uint16(tt.y) {
			t.Errorf("%d. y = %d; want %d", i, tk.y(), tt.y)
		}
	}
}
