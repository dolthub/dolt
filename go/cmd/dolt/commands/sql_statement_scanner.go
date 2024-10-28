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
	"unicode"
)

type statementScanner struct {
	*bufio.Scanner
	statementStartLine int // the line number of the first line of the last parsed statement
	startLineNum       int // the line number we began parsing the most recent token at
	lineNum            int // the current line number being parsed
	Delimiter          string
}

const maxStatementBufferBytes = 100*1024*1024 + 4096
const pageSize = 2 << 11

const (
	sQuote    byte = '\''
	dQuote         = '"'
	backslash      = '\\'
	backtick       = '`'
)

const delimPrefixLen = 10

var delimPrefix = []byte("delimiter ")

// streamScanner is an iterator that reads bytes from |inp| until either
// (1) we match a DELIMITER statement, (2) we match the |delimiter| token,
// or (3) we EOF the file. After each Scan() call, the valid token will
// span from the buffer beginning to |state.end|.
type streamScanner struct {
	inp       io.Reader
	buf       []byte
	maxSize   int
	i         int // leading byte
	fill      int
	err       error
	isEOF     bool
	delimiter []byte
	lineNum   int
	state     *qState
}

func newStreamScanner(r io.Reader) *streamScanner {
	return &streamScanner{inp: r, buf: make([]byte, pageSize), maxSize: maxStatementBufferBytes, delimiter: []byte(";"), state: new(qState)}
}

type qState struct {
	start                          int
	end                            int  // token end, usually i - len(delimiter)
	quoteChar                      byte // the opening quote character of the current quote being parsed, or 0 if the current parse location isn't inside a quoted string
	lastChar                       byte // the last character parsed
	ignoreNextChar                 bool // whether to ignore the next character
	numConsecutiveBackslashes      int  // the number of consecutive backslashes encountered
	seenNonWhitespaceChar          bool // whether we have encountered a non-whitespace character since we returned the last token
	numConsecutiveDelimiterMatches int  // the consecutive number of characters that have been matched to the delimiter
	statementStartLine             int
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
	if !s.skipWhitespace() {
		return false
	}
	s.truncate()

	s.state.statementStartLine = s.lineNum + 1

	if err, ok := s.isDelimiterExpr(); err != nil {
		s.err = err
		return false
	} else if ok {
		// empty token acks DELIMITER
		return true
	}

	for {
		if err, ok := s.seekDelimiter(); err != nil {
			s.err = err
			return false
		} else if ok {
			// delimiter found, scanner holds valid token state
			return true
		} else if s.isEOF && s.i == s.fill {
			// token terminates with file
			s.state.end = s.fill
			return true
		}
		// haven't found delimiter yet, keep reading
		if err := s.read(); err != nil {
			s.err = err
			return false
		}
	}
}

func (s *streamScanner) skipWhitespace() bool {
	for {
		if s.i >= s.fill {
			if err := s.read(); err != nil {
				s.err = err
				return false
			}
		}
		if s.isEOF {
			return true
		}
		if !unicode.IsSpace(rune(s.buf[s.i])) {
			break
		}
		if s.buf[s.i] == '\n' {
			s.lineNum++
		}
		s.i++
	}
	return true
}

func (s *streamScanner) truncate() {
	// copy size should be 4k or less
	s.state.start = s.i
	s.state.end = s.i
}

func (s *streamScanner) resetState() {
	s.state = &qState{}
}

func (s *streamScanner) read() error {
	if s.fill >= s.maxSize {
		// if script exceeds buffer that's OK, if
		// a single query exceeds buffer that's not OK
		if s.state.start == 0 {
			return fmt.Errorf("exceeded max query size")
		}
		// discard previous queries, resulting buffer will start
		// at the current |start|
		s.fill -= s.state.start
		s.i -= s.state.start
		s.state.end = s.state.start
		copy(s.buf[:], s.buf[s.state.start:])
		s.state.start = 0
		return s.read()
	}
	if s.fill == len(s.buf) {
		newBufSize := min(len(s.buf)*2, s.maxSize)
		newBuf := make([]byte, newBufSize)
		copy(newBuf, s.buf)
		s.buf = newBuf
	}
	n, err := s.inp.Read(s.buf[s.fill:])
	if err == io.EOF {
		s.isEOF = true
	} else if err != nil {
		return err
	}
	s.fill += n
	return nil
}

func (s *streamScanner) Err() error {
	return s.err
}

func (s *streamScanner) Bytes() []byte {
	return s.buf[s.state.start:s.state.end]
}

// Text returns the most recent token generated by a call to [Scanner.Scan]
// as a newly allocated string holding its bytes.
func (s *streamScanner) Text() string {
	return string(s.Bytes())
}

func (s *streamScanner) isDelimiterExpr() (error, bool) {
	if s.i == 0 && s.fill-s.i < delimPrefixLen {
		// need to see first |delimPrefixLen| characters
		if err := s.read(); err != nil {
			s.err = err
			return err, false
		}
	}

	// valid delimiter state machine check
	//  "DELIMITER " -> 0+ spaces -> <delimiter string> -> 1 space
	if s.fill-s.i >= delimPrefixLen && bytes.EqualFold(s.buf[s.i:s.i+delimPrefixLen], delimPrefix) {
		delimTokenIdx := s.i
		s.i += delimPrefixLen
		if !s.skipWhitespace() {
			return nil, false
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
		s.delimiter = make([]byte, delimEnd-delimStart)
		copy(s.delimiter, s.buf[delimStart:delimEnd])

		// discard delimiter token, return empty token
		s.truncate()
		return nil, true
	}
	return nil, false
}

func (s *streamScanner) seekDelimiter() (error, bool) {
	if s.i >= s.fill {
		return nil, false
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
			case '\n':
				s.lineNum++
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
					if i+1 >= s.fill {
						// require lookahead or EOF
						if err := s.read(); err != nil {
							return err, false
						}
					}

					// end quote or two consecutive quote characters (a form of escaping quote chars)
					if s.state.quoteChar == s.buf[i] {
						var nextChar byte = 0
						if i+1 < s.fill {
							nextChar = s.buf[i+1]
						}

						if nextChar == s.state.quoteChar {
							// escaped quote. skip the next character
							s.state.ignoreNextChar = true
						} else {
							// end quote
							s.state.quoteChar = 0
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
