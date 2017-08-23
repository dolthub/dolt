// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bidi

import (
	"flag"
	"testing"

	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/internal/gen"
	"gx/ipfs/Qmaau1d1WjnQdTYfRYfFVsCS97cgD8ATyrKuNoEfexL7JZ/go-text/internal/ucd"
)

var long = flag.Bool("long", false,
	"run time-consuming tests, such as tests that fetch data online")

var labels = []string{
	_AL:  "AL",
	_AN:  "AN",
	_B:   "B",
	_BN:  "BN",
	_CS:  "CS",
	_EN:  "EN",
	_ES:  "ES",
	_ET:  "ET",
	_L:   "L",
	_NSM: "NSM",
	_ON:  "ON",
	_R:   "R",
	_S:   "S",
	_WS:  "WS",

	_LRO: "LRO",
	_RLO: "RLO",
	_LRE: "LRE",
	_RLE: "RLE",
	_PDF: "PDF",
	_LRI: "LRI",
	_RLI: "RLI",
	_FSI: "FSI",
	_PDI: "PDI",
}

func TestTables(t *testing.T) {
	if !*long {
		return
	}

	gen.Init()

	trie := newBidiTrie(0)

	ucd.Parse(gen.OpenUCDFile("BidiBrackets.txt"), func(p *ucd.Parser) {
		r1 := p.Rune(0)
		want := p.Rune(1)

		e, _ := trie.lookupString(string(r1))
		if got := entry(e).reverseBracket(r1); got != want {
			t.Errorf("Reverse(%U) = %U; want %U", r1, got, want)
		}
	})

	done := map[rune]bool{}
	test := func(name string, r rune, want string) {
		e, _ := trie.lookupString(string(r))
		if got := labels[entry(e).class(r)]; got != want {
			t.Errorf("%s:%U: got %s; want %s", name, r, got, want)
		}
		done[r] = true
	}

	// Insert the derived BiDi properties.
	ucd.Parse(gen.OpenUCDFile("extracted/DerivedBidiClass.txt"), func(p *ucd.Parser) {
		r := p.Rune(0)
		test("derived", r, p.String(1))
	})
	visitDefaults(func(r rune, c class) {
		if !done[r] {
			test("default", r, labels[c])
		}
	})

}
