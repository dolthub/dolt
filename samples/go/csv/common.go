// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"fmt"
	"unicode/utf8"
)

// StringToRune returns the rune contained in delimiter or an error.
func StringToRune(delimiter string) (rune, error) {
	dlimLen := len(delimiter)
	if dlimLen == 0 {
		return 0, fmt.Errorf("delimiter flag must contain exactly one character (rune), not an empty string")
	}

	d, runeSize := utf8.DecodeRuneInString(delimiter)
	if d == utf8.RuneError {
		return 0, fmt.Errorf("Invalid utf8 string in delimiter flag: %s", delimiter)
	}
	if dlimLen != runeSize {
		return 0, fmt.Errorf("delimiter flag is too long. It must contain exactly one character (rune), but instead it is: %s", delimiter)
	}
	return d, nil
}
