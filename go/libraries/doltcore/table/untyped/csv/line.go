// Copyright 2019 Liquidata, Inc.
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

package csv

import (
	"errors"
	"math"
	"strings"
)

func csvSplitLineRuneDelim(str string, delim rune, escapedQuotes bool) ([]*string, error) {
	return csvSplitLine(str, string(delim), escapedQuotes)
}

func csvSplitLine(str string, delim string, escapedQuotes bool) ([]*string, error) {
	if strings.IndexRune(delim, '"') != -1 {
		panic("delims cannot contain quotes")
	}

	var tokens []*string
	delimLen := len(delim)

	done := false
	escaped := false
	currPos := 0
	cellStart := 0
	for !done {
		remainingStr := str[currPos:]
		nextQuote := strings.Index(remainingStr, "\"")
		nextDelim := strings.Index(remainingStr, delim)

		if nextQuote == -1 || !escapedQuotes {
			nextQuote = math.MaxInt32
		}

		if !escaped && nextDelim < nextQuote {
			if nextDelim == -1 {
				nextDelim = len(remainingStr)
				done = true
			}

			tokens = appendToken(tokens, str, cellStart, currPos+nextDelim, escapedQuotes)
			cellStart = currPos + nextDelim + delimLen
			currPos = cellStart
		} else if escapedQuotes && nextQuote != -1 && nextQuote != math.MaxInt32 {
			escaped = !escaped
			currPos += nextQuote + 1
		} else {
			if escapedQuotes {
				return nil, errors.New(str[cellStart:] + ` has an unclosed quotation mark`)
			}
			break
		}
	}

	return tokens, nil
}

func appendToken(tokens []*string, line string, start, pos int, escapedQuotes bool) []*string {
	if pos == start {
		return append(tokens, nil)
	}

	for isWhitespace(line[start]) {
		start++
	}

	for pos-1 >= 0 && pos-1 < len(line) {
		if !isWhitespace(line[pos-1]) {
			break
		}
		pos--
	}

	if escapedQuotes {
		if line[start] == '"' && line[pos-1] == '"' {
			start++
			pos--
		} else {
			escapedQuotes = false
			if start == pos {
				return append(tokens, nil)
			}
		}
	}

	if !escapedQuotes {
		startToPosNoQuotes := line[start:pos]
		return append(tokens, &startToPosNoQuotes)
	}

	token := make([]byte, len(line)-start)

	end := 0
	for i := start; i < pos; i++ {
		c := line[i]

		if c == '"' {
			if i+1 < len(line) && line[i+1] == '"' {
				token[end] = c
				end++
				i++
			}
		} else {
			token[end] = c
			end++
		}
	}

	s := string(token[:end])
	return append(tokens, &s)
}

func isWhitespace(c uint8) bool {
	return c == ' ' || c == '\n' || c == '\t' || c == '\r'
}
