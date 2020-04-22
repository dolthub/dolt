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
	numConsecutiveQuoteChars int
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
		switch data[i] {
		case '\n':
			s.lineNum++
		case ';':
			if s.quoteChar == 0 {
				return i + 1, data[0:i], nil
			}
		case backslash:
			// escaped quote character
			if s.lastChar == backslash {
				break
			}
		case sQuote, dQuote:
			// escaped quote character
			if s.lastChar == backslash {
				break
			}

			// two quotes in a row
			if s.lastChar == data[i] {
				s.numConsecutiveQuoteChars++
				if s.numConsecutiveQuoteChars % 2 == 1 {
					// escaped quote character
				}
				break
			}

			// end quote
			if s.quoteChar == data[i] {
				s.quoteChar = 0
				s.numConsecutiveQuoteChars = 0
				break
			}

			// embedded quote
			if s.quoteChar != data[i] {
				break
			}

			// open quote
			s.quoteChar = data[i]
		default:
			s.numConsecutiveQuoteChars = 0
		}

		s.lastChar = data[i]
	}

	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}

	// Request more data.
	return 0, nil, nil
}
