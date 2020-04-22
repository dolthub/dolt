package commands

import (
	"bufio"
	"io"
)

type statementScanner struct {
	*bufio.Scanner
	lastStatementLineNum int
	lineNum int
	quoteChar byte
	lastChar byte
	numConsecutiveBackslashes int
	ignoreNextChar bool;
}

func NewSqlStatementScanner(input io.Reader) *statementScanner {
	scanner := bufio.NewScanner(input)
	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	s := &statementScanner{
		Scanner: scanner,
	}
	scanner.Split(s.scanStatements)

	return s
}

func (s *statementScanner) Scan() bool {
	s.lineNum++
	scanned := s.Scanner.Scan()
	return scanned
}

const (
	sQuote byte = '\''
	dQuote = '"'
	backslash = '\\'
)

// ScanStatements is a split function for a Scanner that returns each SQL statement in the input as a token.
func (s *statementScanner) scanStatements(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	for i := range data {

		if !s.ignoreNextChar {
			switch data[i] {
			case '\n':
				s.lineNum++
			case ';':
				if s.quoteChar == 0 {
					_, _, _ = s.resetState()
					return i + 1, data[0:i], nil
				}
			case backslash:
				s.numConsecutiveBackslashes++
			case sQuote, dQuote:
				numConsecutiveBackslashes := s.numConsecutiveBackslashes
				s.numConsecutiveBackslashes = 0

				// escaped quote character
				if s.lastChar == backslash && numConsecutiveBackslashes%2 == 1 {
					break
				}

				// currently in a quoted string
				if s.quoteChar != 0 {

					// end quote or two consecutive quote characters (a form of escaping quote chars)
					if s.quoteChar == data[i] {
						var nextChar byte = 0
						if i+1 < len(data) {
							nextChar = data[i+1]
						}

						if nextChar == s.quoteChar {
							// escaped quote. skip the next character
							s.ignoreNextChar = true
							break
						} else if atEOF || i+1 < len(data) {
							// end quote
							s.quoteChar = 0
							break
						} else {
							// need more data to make a decision
							return s.resetState()
						}
					}

					// embedded quote ('"' or "'")
					break
				}

				// open quote
				s.quoteChar = data[i]
			default:
				s.numConsecutiveBackslashes = 0
			}
		} else {
			s.ignoreNextChar = false
		}

		s.lastChar = data[i]
	}

	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}

	// Request more data.
	return s.resetState()
}

// resetState resets the internal state of the scanner and returns the "more data" response for a split function
func (s *statementScanner) resetState() (advance int, token []byte, err error) {
	s.quoteChar = 0
	s.numConsecutiveBackslashes = 0
	s.ignoreNextChar = false
	s.lastChar = 0
	return 0, nil, nil
}