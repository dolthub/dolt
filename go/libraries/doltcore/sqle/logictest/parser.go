package logictest

import (
	"bufio"
	"errors"
	"io"
	"os"
	"regexp"
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
	result []string
}

func ParseTestFile(f string) ([]*Record, error) {
	file, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	var records []*Record

	scanner := bufio.NewScanner(file)

	for {
		record, err := parseRecord(scanner)
		if err == io.EOF {
			return records, nil
		} else if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

type recordParseState int
const (
	stateStart recordParseState = iota
	stateStatement
	stateQuery
	stateResults
)

var commentRegex = regexp.MustCompile("(.*)#?.*")

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
func parseRecord(scanner *bufio.Scanner) (*Record, error) {
	record := &Record{}

	state := stateStart
	queryBuilder := strings.Builder{}
	for scanner.Scan() {
		line := scanner.Text()
		isBlankLine := isBlankLine(line)
		commentsRemoved := commentRegex.ReplaceAllString(line, "$1")

		fields := strings.Fields(commentsRemoved)
		if len(fields) == 1 && fields[0] == halt {
			return nil, io.EOF
		}

		switch state {
		case stateStart:
			if isBlankLine {
				return nil, errors.New("Unexpected blank line")
			}

			switch fields[0] {
			case skipif, onlyif, hashThreshold:
				// unhandled for now
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
				return nil, errors.New("Unhandled statement " + fields[0])
			}

		case stateStatement:
			if isBlankLine {
				return record, nil
			}

			record.query = commentsRemoved
		case stateQuery:
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

	if scanner.Err() == nil {
		return record, io.EOF
	}

	return record, scanner.Err()
}

func isBlankLine(line string) bool {
	return len(strings.TrimSpace(line)) == 0
}