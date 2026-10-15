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
	"bytes"
	"fmt"
	"io"
	"unicode"
)

const maxStatementBufferBytes = 100*1024*1024 + 4096
const pageSize = 2 << 11

const (
	sQuote    byte = '\''
	dQuote         = '"'
	backslash      = '\\'
	backtick       = '`'
	hyphen         = '-'
	hashSign       = '#'
	asterisk       = '*'
	slash          = '/'
	newline        = '\n'
)

const delimPrefixLen = 10

var delimPrefix = []byte("delimiter ")

// StreamScanner is an iterator that reads bytes from |inp| until either
// (1) we match a DELIMITER statement, (2) we match the |delimiter| token,
// or (3) we EOF the file. After each Scan() call, the valid token will
// span from the buffer beginning to |state.end|.
type StreamScanner struct {
	inp       io.Reader
	err       error
	state     qState
	buf       []byte
	delimiter []byte
	maxSize   int
	i         int // current byte pointer
	fill      int
	lineNum   int
	isEOF     bool

	// splitCommands ends a token before each unquoted backslash.
	splitCommands bool
	discarded     int // bytes discarded from the front of buf
}

// NewStreamScanner returns a new StreamScanner
func NewStreamScanner(r io.Reader) *StreamScanner {
	return &StreamScanner{inp: r, buf: make([]byte, pageSize), maxSize: maxStatementBufferBytes, delimiter: []byte(";")}
}

// newCommandScanner returns a StreamScanner that splits at backslash commands.
func newCommandScanner(r io.Reader) *StreamScanner {
	s := NewStreamScanner(r)
	s.splitCommands = true
	return s
}

// isCommand reports whether the current token is a backslash command.
func (s *StreamScanner) isCommand() bool {
	return s.state.start < s.state.end && s.buf[s.state.start] == backslash
}

// commandFollowsOnLine reports whether a command follows on the same line. It
// advances s.i over the horizontal whitespace, so the next Scan starts there.
func (s *StreamScanner) commandFollowsOnLine() bool {
	for ; ; s.i++ {
		if s.i >= s.fill {
			// read could discard the token's bytes when the buffer is full.
			if s.fill >= s.maxSize || s.read() != nil || s.i >= s.fill {
				return false
			}
		}
		switch s.buf[s.i] {
		case ' ', '\t', '\r', '\v', '\f':
		case backslash:
			return true
		default:
			return false
		}
	}
}

type qState struct {
	start                          int
	end                            int // token end, usually i - len(delimiter)
	numConsecutiveBackslashes      int // the number of consecutive backslashes encountered
	numConsecutiveDelimiterMatches int // the consecutive number of characters that have been matched to the delimiter
	statementStartLine             int
	lineCommentStart               int
	quoteChar                      byte // the opening quote character of the current quote being parsed, or 0 if the current parse location isn't inside a quoted string
	lastChar                       byte // the last character parsed
	ignoreNextChar                 bool // whether to ignore the next character
	insideBlockComment             bool
	insideLineComment              bool

	// endsStatement reports that the token ends a statement, at a delimiter
	// or at the end of input.
	endsStatement bool
}

func (qs qState) insideComment() bool {
	return qs.insideLineComment || qs.insideBlockComment
}

func (qs qState) insideQuote() bool {
	return qs.quoteChar != 0
}

// ignoreDelimiters returns if delimiters should be ignored. If inside a comment or a quote, delimiters, including
// comment delimiters, should be ignored
func (qs qState) ignoreDelimiters() bool {
	return qs.insideComment() || qs.insideQuote()
}

func (s *StreamScanner) Scan() bool {
	s.state = qState{}

	if s.i >= s.fill && !s.isEOF {
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
			s.state.endsStatement = true
			return true
		}
		// haven't found delimiter yet, keep reading
		if err := s.read(); err != nil {
			s.err = err
			return false
		}
	}
}

func (s *StreamScanner) skipWhitespace() bool {
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

func (s *StreamScanner) truncate() {
	// copy size should be 4k or less
	s.state.start = s.i
	s.state.end = s.i
}

func (s *StreamScanner) read() error {
	if s.fill >= s.maxSize {
		// if script exceeds buffer that's OK, if
		// a single query exceeds buffer that's not OK
		if s.state.start == 0 {
			return fmt.Errorf("exceeded max query size")
		}
		// discard previous queries, resulting buffer will start
		// at the current |start|
		s.discarded += s.state.start
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

func (s *StreamScanner) Err() error {
	return s.err
}

func (s *StreamScanner) Bytes() []byte {
	return s.buf[s.state.start:s.state.end]
}

// Text returns the most recent token generated by a call to [Scanner.Scan]
// as a newly allocated string holding its bytes.
func (s *StreamScanner) Text() string {
	return string(s.Bytes())
}

func (s *StreamScanner) isDelimiterExpr() (error, bool) {
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

func (s *StreamScanner) seekDelimiter() (error, bool) {
	for ; s.i < s.fill; s.i++ {
		i := s.i
		if !s.state.ignoreNextChar {
			// check if we've matched the delimiter string
			if !s.state.ignoreDelimiters() && s.buf[i] == s.delimiter[s.state.numConsecutiveDelimiterMatches] {
				s.state.numConsecutiveDelimiterMatches++
				if s.state.numConsecutiveDelimiterMatches == len(s.delimiter) {
					s.state.end = s.i - len(s.delimiter) + 1
					s.state.endsStatement = true
					s.i++
					return nil, true
				}
				s.state.lastChar = s.buf[i]
				continue
			} else {
				s.state.numConsecutiveDelimiterMatches = 0
			}

			switch s.buf[i] {
			case newline:
				// Ends a backslash command at its line. The next Scan counts the newline.
				if s.buf[s.state.start] == backslash {
					s.state.end = i
					return nil, true
				}
				s.lineNum++
				if s.state.insideLineComment {
					s.state.insideLineComment = false
					// if the entire statement is a line comment, truncate and return as empty
					if s.state.start == s.state.lineCommentStart {
						s.i++
						s.truncate()
						return nil, true
					}
				}
			case hyphen:
				// If inside quote or already inside comment, ignore. Otherwise, if previous character is also a hyphen,
				// ie "--", begin line comment.
				if !s.state.ignoreDelimiters() && s.state.lastChar == hyphen {
					s.state.lineCommentStart = i - 1
					s.state.insideLineComment = true
				}
			case hashSign:
				// A hash starts a line comment, unless it is inside a quote or comment.
				if !s.state.ignoreDelimiters() {
					s.state.lineCommentStart = i
					s.state.insideLineComment = true
				}
			case asterisk:
				// If inside quote or already inside comment, ignore. Otherwise, if previous character is a slash, ie
				// "/*", begin block comment.
				if !s.state.ignoreDelimiters() && s.state.lastChar == slash {
					s.state.insideBlockComment = true
				}
			case slash:
				// If previous character is an asterisk, ie "*/", end block comment.
				if s.state.insideBlockComment && s.state.lastChar == asterisk {
					s.state.insideBlockComment = false
				}
			case backslash:
				// Ends the SQL before a command. Inside quotes or comments, \ is text.
				if s.splitCommands && !s.state.ignoreDelimiters() && i > s.state.start && s.buf[s.state.start] != backslash {
					s.state.end = i
					return nil, true
				}
				s.state.numConsecutiveBackslashes++
			case sQuote, dQuote, backtick:
				// ignore quotes inside comments
				if s.state.insideComment() {
					break
				}

				prevNumConsecutiveBackslashes := s.state.numConsecutiveBackslashes
				s.state.numConsecutiveBackslashes = 0

				// escaped quote character
				if s.state.lastChar == backslash && prevNumConsecutiveBackslashes%2 == 1 {
					break
				}

				// currently in a quoted string
				if s.state.insideQuote() {
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
