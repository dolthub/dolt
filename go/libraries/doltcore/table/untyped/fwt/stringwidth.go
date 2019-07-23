package fwt

import (
	"github.com/mattn/go-runewidth"
	"github.com/rivo/uniseg"
)

// StringWidth returns the number of horizontal cells needed to print the given text. It splits the text into its
// grapheme clusters, calculates each cluster's width, and adds them up to a total.
func StringWidth(text string) (width int) {
	g := uniseg.NewGraphemes(text)
	for g.Next() {
		var chWidth int
		for _, r := range g.Runes() {
			chWidth = runewidth.RuneWidth(r)
			if chWidth > 0 {
				break // Our best guess at this point is to use the width of the first non-zero-width rune.
			}
		}
		width += chWidth
	}
	return
}
