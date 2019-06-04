package fwt

import (
	"github.com/mattn/go-runewidth"
  "github.com/rivo/uniseg"
	"strings"
)

// StringWidth returns the number of horizontal cells needed to print the given text. It splits the text into lines,
// each line into grapheme clusters, calculates each cluster's width, sums them, and returns the width of the
// longest line
func StringWidth(text string) int {
	var maxWidth int
	lines := strings.Split(text, "\n")

	for _, line := range lines {
		var width int
		g := uniseg.NewGraphemes(line)

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

		if width > maxWidth {
			maxWidth = width
		}
	}

	return maxWidth
}
