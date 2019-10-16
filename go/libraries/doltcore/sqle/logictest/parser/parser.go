package parser

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
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
	// The type of this record
	recordType RecordType
	// Whether this record expects an error to occur on execution.
	expectError bool
	// The condition for executing this record, if applicable
	condition *Condition
	// The schema for results of this query record, in the form e.g. "ITTR"
	schema string
	// The sort mode for validating results of a query
	sortMode SortMode
	// The query string or statement to execute
	query string
	// The canonical line number for this record, which is the first line number of the SQL statement or
	// query to execute.
	lineNum int
	// The expected result of the query, represented as a strings
	result []string
	// Label used to store results for a query, currently unused.
	label string
}

type Condition struct {
	isOnly bool
	isSkip bool
	engine string
}

var hashRegex = regexp.MustCompile("(\\d+) values hashing to ([0-9a-f]+)")

type RecordType int
const (
	// Statement is a record to execute with no results to validate, such as create or insert
	Statement RecordType = iota
	// Query is a record to execute and validate that results are as expected
	Query
	// Halt is a record that terminates the current test script's execution
	Halt
)

// Type returns the type of this record.
func (r *Record) Type() RecordType {
	return r.recordType
}

// ShouldExecuteForEngine returns whether this record should be executed for the engine with the identifier given.
func (r *Record) ShouldExecuteForEngine(engine string) bool {
	if r.condition == nil {
		return true
	}

	if r.condition.isOnly {
		return r.condition.engine == engine
	} else if r.condition.isSkip {
		return r.condition.engine != engine
	} else {
		panic("Incorrectly constructed condition for record: one of isSkip, isOnly must be true")
	}
}

// ExpectError returns whether this record expects an error to occur on execution.
func (r *Record) ExpectError() bool {
	return r.expectError
}

// Schema returns the schema for the results of this query, in the form e.g. "ITTR"
func (r *Record) Schema() string {
	return r.schema
}

// Query returns the query for this record, which is either a statement to execute or a query to validate results for.
func (r *Record) Query() string {
	return r.query
}

// Returns the expected results of the query for this record. For many records, this is a hash of sorted results
// instead of the full list of values. Use IsHashResult to disambiguate.
func (r *Record) Result() []string {
	return r.result
}

// IsHashResult returns whether this record has a hash result (as opposed to enumerating each value).
func (r *Record) IsHashResult() bool {
	return len(r.result) == 1 && hashRegex.MatchString(r.result[0])
}

// HashResult returns the hash for result values for this record.
func (r *Record) HashResult() string {
	return hashRegex.ReplaceAllString(r.result[0], "$2")
}

// NumRows returns the number of results (not rows) for this record. Panics if the record is a statement instead of a
// query.
func (r *Record) NumResults() int {
	if r.recordType != Query {
		panic("Only query records have results")
	}

	numVals := len(r.result)
	if r.IsHashResult() {
		valsStr := hashRegex.ReplaceAllString(r.result[0], "$1")
		numVals, _ = strconv.Atoi(valsStr)
	}

	return numVals / len(r.schema)
}

// NumCols returns the number of columns for results of this record's query. Panics if the record is a statement instead
// of a query.
func (r *Record) NumCols() int {
	if r.recordType != Query {
		panic("Only query records have results")
	}

	return len(r.schema)
}

// LineNum returns the canonical line number for this record, which is the first line number of the SQL statement or
// query to execute.
func (r *Record) LineNum() int {
	return r.lineNum
}

type lineScanner struct {
	*bufio.Scanner
	lineNum int
}

func (ls *lineScanner) Scan() bool {
	ls.lineNum++
	return ls.Scanner.Scan()
}

// rowSorter sorts a slice of result values with by-row semantics.
type rowSorter struct {
	record *Record
	values []string
}

func (s rowSorter) toRow(i int) []string {
	return s.values[i*s.record.NumCols():(i+1)*s.record.NumCols()]
}

func (s rowSorter) Len() int {
	return len(s.values) / s.record.NumCols()
}

func (s rowSorter) Less(i, j int) bool {
	rowI := s.toRow(i)
	rowJ := s.toRow(j)
	for k := range rowI {
		if rowI[k] < rowJ[k] {
			return true
		}
		if rowI[k] > rowJ[k] {
			return false
		}
	}
	return false
}

func (s rowSorter) Swap(i, j int) {
	rowI := s.toRow(i)
	rowJ := s.toRow(j)
	for col := range rowI {
		rowI[col], rowJ[col] = rowJ[col], rowI[col]
	}
}

// Sort results sorts the input slice (the results of this record's query) according to the record's specification
// (no sorting, row-based sorting, or value-based sorting) and returns it.
func (r *Record) SortResults(results []string) []string {
	switch r.sortMode {
	case NoSort:
		return results
	case Rowsort:
		sorter := rowSorter{
			record: r,
			values: results,
		}
		sort.Sort(sorter)
		return sorter.values
	case ValueSort:
		sort.Strings(results)
		return results
	default:
		panic(fmt.Sprintf("Uncrecognized sort mode %v", r.sortMode))
	}
}

// ParseTestFile parses a sqllogictest file and returns the array of records it contains, or an error if it cannot.
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

		switch state {
		case stateStart:
			if isBlankLine {
				continue
			}

			switch fields[0] {
			case halt:
				record.recordType = Halt
				record.lineNum = scanner.lineNum
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
				return nil, fmt.Errorf("Unhandled statement %s on line %d", fields[0], scanner.lineNum)
			}

		case stateStatement:
			if isBlankLine {
				return record, nil
			}

			record.query = commentsRemoved
			record.lineNum = scanner.lineNum
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

	if scanner.Err() == nil && linesScanned == 0 {
		return nil, io.EOF
	}

	return record, nil
}

func isBlankLine(line string) bool {
	return len(strings.TrimSpace(line)) == 0
}