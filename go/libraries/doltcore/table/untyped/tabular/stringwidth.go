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

package tabular

import (
	"github.com/mattn/go-runewidth"
	"github.com/rivo/uniseg"
)

// FixedWidthString contains all of the information needed to properly display a multiline string in tabular mode.
type FixedWidthString struct {
	TotalWidth   int // The combined display width of every line
	DisplayWidth int // The display width of the longest line
	Lines        []DisplayLine
}

// DisplayLine contains the information for each line of a string.
type DisplayLine struct {
	Width     int // Width is the display width of this specific line.
	ByteStart int // ByteStart is the beginning offset of the original string that this line represents.
	ByteEnd   int // ByteEnd is the ending offset of the original string that this line represents (excludes newline).
}

// NewFixedWidthString returns the number of horizontal cells needed to print the given text. It splits the text into its
// grapheme clusters, calculates each cluster's width, and adds them up to a total.
func NewFixedWidthString(text string) FixedWidthString {
	// An empty string will still have a single line, it will just be empty.
	displayString := FixedWidthString{
		TotalWidth:   0,
		DisplayWidth: 0,
		Lines:        []DisplayLine{{0, 0, 0}},
	}
	line := &displayString.Lines[0]
	g := uniseg.NewGraphemes(text)
	for g.Next() {
		var chWidth int
		for i, r := range g.Runes() {
			// Newline has been encountered so start a new line
			if i == 0 && r == '\n' {
				// Check if this line is the longest line
				if line.Width > displayString.DisplayWidth {
					displayString.DisplayWidth = line.Width
				}
				// Create a new line
				start, _ := g.Positions()
				line.ByteEnd = start
				displayString.Lines = append(displayString.Lines, DisplayLine{
					Width:     0,
					ByteStart: start + 1,
					ByteEnd:   0,
				})
				line = &displayString.Lines[len(displayString.Lines)-1]
			}
			chWidth = runewidth.RuneWidth(r)
			if chWidth > 0 {
				break // Our best guess at this point is to use the width of the first non-zero-width rune.
			}
		}
		displayString.TotalWidth += chWidth
		line.Width += chWidth
	}
	// Set the end position of the last line
	line.ByteEnd = len(text)
	// Check if the last line added is the longest line
	if line.Width > displayString.DisplayWidth {
		displayString.DisplayWidth = line.Width
	}
	return displayString
}

// NewFixedWidthStrings returns a FixedWidthString slice from the given string slice.
func NewFixedWidthStrings(text []string) []FixedWidthString {
	widths := make([]FixedWidthString, len(text))
	for i := range text {
		widths[i] = NewFixedWidthString(text[i])
	}
	return widths
}

// ColoredStringWidth calculates the FixedWidthString for text that has been colored. Text colors interfere with the
// display width calculation, as they're considered displayable characters. This works by calculating the relevant
// information from the colored and uncolored text variants to return the correct FixedWidthString.
func ColoredStringWidth(coloredText string, uncoloredText string) FixedWidthString {
	colored := NewFixedWidthString(coloredText)
	uncolored := NewFixedWidthString(uncoloredText)
	for i := 0; i < min(len(colored.Lines), len(uncolored.Lines)); i++ {
		uncolored.Lines[i].ByteStart = colored.Lines[i].ByteStart
		uncolored.Lines[i].ByteEnd = colored.Lines[i].ByteEnd
	}
	return uncolored
}
