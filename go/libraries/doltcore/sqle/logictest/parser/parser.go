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

package parser

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const (
	separator     = "----"
	halt          = "halt"
	hashThreshold = "hash-threshold"
	skipif        = "skipif"
	onlyif        = "onlyif"
)

// ParseTestFile parses a sqllogictest file and returns the array of records it contains, or an error if it cannot.
func ParseTestFile(f string) ([]*Record, error) {
	file, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	var records []*Record

	scanner := LineScanner{bufio.NewScanner(file), 0}

	for {
		record, err := parseRecord(&scanner)
		if err == io.EOF {
			return records, nil
		} else if err != nil {
			return nil, err
		}
		if record != nil {
			records = append(records, record)
		}
	}
}

type recordParseState int

const (
	stateStart recordParseState = iota
	stateStatement
	stateQuery
	stateResults
)

var commentRegex = regexp.MustCompile("([^#]*)#?.*")

// Parses a test record, the format of which is described here:
// https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
// Example record:
// query III nosort
// SELECT a,
// c-d,
// d
// FROM t1
// WHERE c>d
// AND a>b
// AND (a>b-2 AND a<b+2)
// ORDER BY 1,2,3
// ----
// 131
// 1
// 133
// 182
// 1
// 183
// For control records, returns (nil, nil) on hash-threshold and (nil, EOF) for halt.
func parseRecord(scanner *LineScanner) (*Record, error) {
	record := &Record{}

	state := stateStart
	queryBuilder := strings.Builder{}
	linesScanned := 0

	for scanner.Scan() {
		line := scanner.Text()
		linesScanned++
		isBlankLine := isBlankLine(line)
		commentsRemoved := commentRegex.ReplaceAllString(line, "$1")

		// skip lines that are entirely comments
		if strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(commentsRemoved)

		switch state {
		case stateStart:
			if isBlankLine {
				continue
			}

			switch fields[0] {
			case halt:
				record.recordType = Halt
				record.lineNum = scanner.LineNum
				return record, nil
			case skipif, onlyif:
				record.condition = &Condition{
					isOnly: fields[0] == onlyif,
					isSkip: fields[0] == skipif,
					engine: fields[1],
				}
			case hashThreshold:
				// Ignored
				return nil, nil
			case "statement":
				record.recordType = Statement
				if fields[1] == "ok" {
					record.expectError = false
				} else if fields[1] == "error" {
					record.expectError = true
				} else {
					return nil, errors.New("unexpected token " + fields[1])
				}
				state = stateStatement
			case "query":
				record.recordType = Query
				record.schema = fields[1]
				if len(fields) > 2 {
					record.sortMode = SortMode(fields[2])
				}
				if len(fields) > 3 {
					record.label = fields[3]
				}
				state = stateQuery
			default:
				return nil, fmt.Errorf("Unhandled statement %s on line %d", fields[0], scanner.LineNum)
			}

		case stateStatement:
			if isBlankLine {
				return record, nil
			}

			record.query = commentsRemoved
			record.lineNum = scanner.LineNum
			return record, nil
		case stateQuery:
			if record.lineNum == 0 {
				record.lineNum = scanner.LineNum
			}

			if len(fields) == 1 && fields[0] == separator {
				record.query = queryBuilder.String()
				state = stateResults
			} else if isBlankLine {
				record.query = queryBuilder.String()
				return record, nil
			}

			queryBuilder.WriteString(commentsRemoved)
		case stateResults:
			if isBlankLine {
				return record, nil
			}

			record.result = append(record.result, commentsRemoved)
		}
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	if scanner.Err() == nil && linesScanned == 0 {
		return nil, io.EOF
	}

	return record, nil
}

func isBlankLine(line string) bool {
	return len(strings.TrimSpace(line)) == 0
}
