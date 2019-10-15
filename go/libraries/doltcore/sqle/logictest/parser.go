package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type SortMode string

const (
	NoSort SortMode = "nosort"
	Rowsort SortMode = "rowsort"
	ValueSort SortMode = "valuesort"
)

const (
	separator     = "----"
	halt          = "halt"
	hashThreshold = "hash-threshold"
	skipif        = "skipif"
	onlyif        = "onlyif"
)

// A test script contains many records, which can be either statements to execute or queries with results.
type Record struct {
	isStatement bool
	expectError bool
	schema string // string-based schema, such as ITTR
	sortMode SortMode
	label string
	query string
	lineNum int
	result []string
}

var hashRegex = regexp.MustCompile("(\\d+) values hashing to ([0-9a-f]+)")

func (r *Record) NumRows() int {
	if r.isStatement {
		panic("No result rows for a statement record")
	}

	numVals := len(r.result)
	if r.IsHashResult() {
		valsStr := hashRegex.ReplaceAllString(r.result[0], "$1")
		numVals, _ = strconv.Atoi(valsStr)
	}

	return numVals / len(r.schema)
}

func (r *Record) NumCols() int {
	if r.isStatement {
		panic("No result rows for a statement record")
	}

	return len(r.schema)
}

func (r *Record) LineNum() int {
	return r.lineNum
}

func (r *Record) IsHashResult() bool {
	return len(r.result) == 1 && hashRegex.MatchString(r.result[0])
}

func (r *Record) HashResult() string {
	return hashRegex.ReplaceAllString(r.result[0], "$2")
}

type lineScanner struct {
	*bufio.Scanner
	lineNum int
}

func (ls *lineScanner) Scan() bool {
	ls.lineNum++
	return ls.Scanner.Scan()
}

func ParseTestFile(f string) ([]*Record, error) {
	file, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	var records []*Record

	scanner := lineScanner{bufio.NewScanner(file), 0 }

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
func parseRecord(scanner *lineScanner) (*Record, error) {
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
		if len(fields) == 1 && fields[0] == halt {
			return nil, io.EOF
		}

		switch state {
		case stateStart:
			if isBlankLine {
				return nil, fmt.Errorf("Unexpected blank line on line %d", scanner.lineNum)
			}

			switch fields[0] {
			case skipif, onlyif:
			// unhandled for now
			case hashThreshold:
				// Advance the scanner past the following blank line
				scanner.Scan()
				return nil, nil
			case "statement":
				record.isStatement = true
				if fields[1] == "ok" {
					record.expectError = false
				} else if fields[1] == "error" {
					record.expectError = true
				} else {
					return nil, errors.New("unexpected token " + fields[1])
				}
				state = stateStatement
			case "query":
				record.schema = fields[1]
				if len(fields) > 2 {
					record.sortMode = SortMode(fields[2])
				}
				if len(fields) > 3 {
					record.label = fields[3]
				}
				state = stateQuery
			default:
				return nil, fmt.Errorf("Unhandled statement %s on line %d", fields[0], scanner.lineNum)
			}

		case stateStatement:
			if isBlankLine {
				return record, nil
			}

			record.query = commentsRemoved
			record.lineNum = scanner.lineNum
			// Advance past the following blank line
			scanner.Scan()
			return record, nil
		case stateQuery:
			if record.lineNum == 0 {
				record.lineNum = scanner.lineNum
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

	if scanner.Err() == nil && linesScanned == 0{
		return nil, io.EOF
	}

	return record, nil
}

func isBlankLine(line string) bool {
	return len(strings.TrimSpace(line)) == 0
}