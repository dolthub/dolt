package csv

func csvSplitLine(str string, delim rune, escapedQuotes bool) []string {
	var tokens []string

	quotations := 0
	escaped := false
	start := 0
	for pos, c := range str {
		if c == delim && !escaped {
			tokens = appendToken(tokens, str, start, pos, quotations)
			start = pos + 1
			quotations = 0

			if pos == len(str)-1 {
				tokens = appendToken(tokens, "", 0, 0, 0)
			}
		} else if escapedQuotes && c == '"' {
			escaped = !escaped
			quotations++
		}
	}

	if start != len(str) {
		tokens = appendToken(tokens, str, start, len(str), quotations)
	}

	return tokens
}

func appendToken(tokens []string, line string, start, pos, quotations int) []string {
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
