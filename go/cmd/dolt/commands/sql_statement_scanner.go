// Copyright 2020 Dolthub, Inc.
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

package commands

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"regexp"
	"unicode"
)

type statementScanner struct {
	*bufio.Scanner
	statementStartLine int // the line number of the first line of the last parsed statement
	startLineNum       int // the line number we began parsing the most recent token at
	lineNum            int // the current line number being parsed
	Delimiter          string
}

const maxStatementBufferBytes = 100 * 1024 * 1024
const pageSize = 2 << 11

func NewSqlStatementScanner(input io.Reader) *statementScanner {
	scanner := bufio.NewScanner(input)
	const initialCapacity = 512 * 1024
	buf := make([]byte, initialCapacity)
	scanner.Buffer(buf, maxStatementBufferBytes)

	s := &statementScanner{
		Scanner:   scanner,
		lineNum:   1,
		Delimiter: ";",
	}
	scanner.Split(s.scanStatements)

	return s
}

const (
	sQuote    byte = '\''
	dQuote         = '"'
	backslash      = '\\'
	backtick       = '`'
)

var scannerDelimiterRegex = regexp.MustCompile(`(?i)^\s*DELIMITER\s+(\S+)\s*`)

const delimPrefixLen = 10

var delimPrefix = []byte("delimiter ")

// ScanStatements is a split function for a Scanner that returns each SQL statement in the input as a token.
func (s *statementScanner) scanStatements(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	log.Printf("\n%d\n", len(data))

	var (
		quoteChar                      byte // the opening quote character of the current quote being parsed, or 0 if the current parse location isn't inside a quoted string
		lastChar                       byte // the last character parsed
		ignoreNextChar                 bool // whether to ignore the next character
		numConsecutiveBackslashes      int  // the number of consecutive backslashes encountered
		seenNonWhitespaceChar          bool // whether we have encountered a non-whitespace character since we returned the last token
		numConsecutiveDelimiterMatches int  // the consecutive number of characters that have been matched to the delimiter
	)

	s.startLineNum = s.lineNum

	if idxs := scannerDelimiterRegex.FindIndex(data); len(idxs) == 2 {
		s.Delimiter = scannerDelimiterRegex.FindStringSubmatch(string(data))[1]
		// Returning a nil token is interpreted as an error condition, so we return an empty token instead
		return idxs[1], []byte{}, nil
	}

	for i := 0; i < len(data); i++ {
		if !ignoreNextChar {
			// this doesn't handle unicode characters correctly and will break on some things, but it's only used for line
			// number reporting.
			if !seenNonWhitespaceChar && !unicode.IsSpace(rune(data[i])) {
				seenNonWhitespaceChar = true
				s.statementStartLine = s.lineNum
			}
			// check if we've matched the delimiter string
			if quoteChar == 0 && data[i] == s.Delimiter[numConsecutiveDelimiterMatches] {
				numConsecutiveDelimiterMatches++
				if numConsecutiveDelimiterMatches == len(s.Delimiter) {
					s.startLineNum = s.lineNum
					_, _, _ = s.resetState()
					removalLength := len(s.Delimiter) - 1 // We remove the delimiter so it depends on the length
					return i + 1, data[0 : i-removalLength], nil
				}
				lastChar = data[i]
				continue
			} else {
				numConsecutiveDelimiterMatches = 0
			}

			switch data[i] {
			case '\n':
				s.lineNum++
			case backslash:
				numConsecutiveBackslashes++
			case sQuote, dQuote, backtick:
				prevNumConsecutiveBackslashes := numConsecutiveBackslashes
				numConsecutiveBackslashes = 0

				// escaped quote character
				if lastChar == backslash && prevNumConsecutiveBackslashes%2 == 1 {
					break
				}

				// currently in a quoted string
				if quoteChar != 0 {

					// end quote or two consecutive quote characters (a form of escaping quote chars)
					if quoteChar == data[i] {
						var nextChar byte = 0
						if i+1 < len(data) {
							nextChar = data[i+1]
						}

						if nextChar == quoteChar {
							// escaped quote. skip the next character
							ignoreNextChar = true
							break
						} else if atEOF || i+1 < len(data) {
							// end quote
							quoteChar = 0
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
				quoteChar = data[i]
			default:
				numConsecutiveBackslashes = 0
			}
		} else {
			ignoreNextChar = false
		}

		lastChar = data[i]
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
	// rewind the line number to where we started parsing this token
	s.lineNum = s.startLineNum
	return 0, nil, nil
}

type streamScanner struct {
	inp                io.Reader
	buf                []byte
	maxSize            int
	i                  int // leading byte
	fill               int
	err                error
	isEOF              bool
	delimiter          []byte
	statementStartLine int
	state              *qState
}

func newStreamScanner(r io.Reader) *streamScanner {
	return &streamScanner{inp: r, buf: make([]byte, pageSize), maxSize: maxStatementBufferBytes, delimiter: []byte(";")}
}

type qState struct {
	end                            int // token end, usually i - len(delimiter)
	checkedDelim                   bool
	quoteChar                      byte // the opening quote character of the current quote being parsed, or 0 if the current parse location isn't inside a quoted string
	lastChar                       byte // the last character parsed
	ignoreNextChar                 bool // whether to ignore the next character
	numConsecutiveBackslashes      int  // the number of consecutive backslashes encountered
	seenNonWhitespaceChar          bool // whether we have encountered a non-whitespace character since we returned the last token
	numConsecutiveDelimiterMatches int  // the consecutive number of characters that have been matched to the delimiter
}

func (s *streamScanner) Scan() bool {
	// truncate last query
	s.truncate()
	s.resetState()

	if s.i >= s.fill {
		// initialize buffer
		if err := s.read(); err != nil {
			s.err = err
			return false
		}
	}

	if s.isEOF || s.i == s.fill {
		// no token
		return false
	}

	// discard leading whitespace
	for ; unicode.IsSpace(rune(s.buf[s.i])); s.i++ {
		if s.i >= s.fill {
			if err := s.read(); err != nil {
				s.err = err
				return false
			}
		}
	}
	s.truncate()

	if err, ok := s.isDelimiterExpr(); err != nil {
		s.err = err
		return false
	} else if ok {
		// empty token is required to ack DELIMITER
		return true
	}

	for {
		if err, ok := s.seekDelimiter(); err != nil {
			s.err = err
			return false
		} else if ok {
			return true
		} else if s.isEOF && s.i == s.fill {
			s.state.end = s.fill
			return true
		} else {
			s.i = s.fill
		}
		if err := s.read(); err != nil {
			s.err = err
			return false
		}
	}
}

func (s *streamScanner) truncate() {
	// copy size should be 4k or less
	copy(s.buf, s.buf[s.i:])
	s.fill -= s.i
	s.i = 0
}

func (s *streamScanner) resetState() {
	s.state = &qState{}
}

func (s *streamScanner) read() error {
	newFill := s.fill + pageSize
	if newFill > len(s.buf) {
		newSize := len(s.buf) * 2
		if newSize > s.maxSize {
			return fmt.Errorf("exceeded max query size")
		}
		newBuf := make([]byte, newSize)
		copy(newBuf, s.buf)
		s.buf = newBuf
	}
	// read at most |pageSize| into |s.buf| at index |s.fill|
	n, err := s.inp.Read(s.buf[s.fill:newFill])
	if err == io.EOF {
		s.isEOF = true
	} else if err != nil {
		return err
	}
	// update fill, to |newFill| in the optimistic case
	s.fill += n
	return nil
}

func (s *streamScanner) Err() error {
	return s.err
}

func (s *streamScanner) Bytes() []byte {
	return s.buf[:s.state.end]
}

// Text returns the most recent token generated by a call to [Scanner.Scan]
// as a newly allocated string holding its bytes.
func (s *streamScanner) Text() string {
	log.Printf("i=%d fill=%d end=%d", s.i, s.fill, s.state.end)
	return string(s.buf[:s.state.end])
}

func (s *streamScanner) isDelimiterExpr() (error, bool) {
	if s.fill-s.i < delimPrefixLen {
		// need to see first 9 characters
		if err := s.read(); err != nil {
			s.err = err
			return err, false
		}
	}

	// valid delimiter state machine check
	//  "DELIMITER " -> 0+ spaces -> <delimiter string> -> 1 space
	s.state.checkedDelim = true

	if s.fill-s.i < delimPrefixLen && bytes.EqualFold(s.buf[s.i:s.i+delimPrefixLen], delimPrefix) {
		delimTokenIdx := s.i
		s.i += delimPrefixLen
		for ; !s.isEOF && unicode.IsSpace(rune(s.buf[s.i])); s.i++ {
			if s.i >= s.fill {
				if err := s.read(); err != nil {
					s.err = err
					return err, false
				}
			}
		}
		if s.isEOF {
			// invalid delimiter
			s.i = delimTokenIdx
			return nil, false
		}
		delimStart := s.i
		for ; !s.isEOF && !unicode.IsSpace(rune(s.buf[s.i])); s.i++ {
			if s.i >= s.fill {
				if err := s.read(); err != nil {
					s.err = err
					return err, false
				}
			}
		}
		delimEnd := s.i
		s.delimiter = s.buf[delimStart:delimEnd]

		// discard delimiter token, return empty token
		s.truncate()
		return nil, true
	}
	return nil, false
}

func (s *streamScanner) seekDelimiter() (error, bool) {
	if s.i >= s.fill {
		return io.EOF, false
	}

	for ; s.i < s.fill; s.i++ {
		i := s.i
		if !s.state.ignoreNextChar {
			// this doesn't handle unicode characters correctly and will break on some things, but it's only used for line
			// number reporting.
			if !s.state.seenNonWhitespaceChar && !unicode.IsSpace(rune(s.buf[i])) {
				s.state.seenNonWhitespaceChar = true
			}
			// check if we've matched the delimiter string
			if s.state.quoteChar == 0 && s.buf[i] == s.delimiter[s.state.numConsecutiveDelimiterMatches] {
				s.state.numConsecutiveDelimiterMatches++
				if s.state.numConsecutiveDelimiterMatches == len(s.delimiter) {
					s.state.end = s.i - len(s.delimiter) + 1
					s.i++
					return nil, true
				}
				s.state.lastChar = s.buf[i]
				continue
			} else {
				s.state.numConsecutiveDelimiterMatches = 0
			}

			switch s.buf[i] {
			case backslash:
				s.state.numConsecutiveBackslashes++
			case sQuote, dQuote, backtick:
				prevNumConsecutiveBackslashes := s.state.numConsecutiveBackslashes
				s.state.numConsecutiveBackslashes = 0

				// escaped quote character
				if s.state.lastChar == backslash && prevNumConsecutiveBackslashes%2 == 1 {
					break
				}

				// currently in a quoted string
				if s.state.quoteChar != 0 {

					// end quote or two consecutive quote characters (a form of escaping quote chars)
					if s.state.quoteChar == s.buf[i] {
						var nextChar byte = 0
						if i+1 < len(s.buf) {
							nextChar = s.buf[i+1]
						}

						if nextChar == s.state.quoteChar {
							// escaped quote. skip the next character
							s.state.ignoreNextChar = true
							break
						} else if s.isEOF || i+1 < s.fill {
							// end quote
							s.state.quoteChar = 0
							break
						} else {
							// need more data to make a decision
							//todo: ??
							//return nil, false
							return nil, false
						}
					}

					// embedded quote ('"' or "'")
					break
				}

				// open quote
				s.state.quoteChar = s.buf[i]
			default:
				s.state.numConsecutiveBackslashes = 0
			}
		} else {
			s.state.ignoreNextChar = false
		}

		s.state.lastChar = s.buf[i]
	}
	return nil, false
}
