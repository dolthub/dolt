package csv

import (
	"math"
	"strings"
)

func csvSplitLineRuneDelim(str string, delim rune, escapedQuotes bool) []string {
	return csvSplitLine(str, string(delim), escapedQuotes)
}

func csvSplitLine(str string, delim string, escapedQuotes bool) []string {
	if strings.IndexRune(delim, '"') != -1 {
		panic("delims cannot contain quotes")
	}

	var tokens []string
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
		} else if escapedQuotes && nextQuote != -1 {
			escaped = !escaped
			currPos += nextQuote + 1
		} else {
			break
		}
	}

	return tokens
}

func appendToken(tokens []string, line string, start, pos int, escapedQuotes bool) []string {
	quotations := 0

	if escapedQuotes {
		for _, c := range line {
			if c == '"' {
				quotations++
			}
		}
	}

	if start == pos {
		return append(tokens, "")
	}

	for isWhitespace(line[start]) {
		start++
	}

	if start == pos {
		return append(tokens, "")
	}

	for isWhitespace(line[pos-1]) {
		pos--
	}

	if quotations == 0 {
		return append(tokens, line[start:pos])
	} else if quotations == 2 && line[start] == '"' && line[pos-1] == '"' {
		return append(tokens, line[start+1:pos-1])
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
	return append(tokens, s)
}

func isWhitespace(c uint8) bool {
	return c == ' ' || c == '\n' || c == '\t' || c == '\r'
}
