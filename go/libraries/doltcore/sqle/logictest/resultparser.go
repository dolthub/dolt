package logictest

import (
	"bufio"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/logictest/parser"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type ResultType int
const (
	Ok ResultType = iota
	NotOk
	Skipped
)

// ResultLogEntry is a single line in a sqllogictest result log file.
type ResultLogEntry struct {
	entryTime time.Time
	testFile string
	lineNum int
	query string
	result ResultType
	errorMessage string
}

// ParseResultFile parses a result log file produced by the test runner and returns a slice of results, in the order
// that they occurred.
func ParseResultFile(f string) ([]*ResultLogEntry, error) {
	file, err := os.Open(f)
	if err != nil {
		panic(err)
	}

	var entries []*ResultLogEntry

	scanner := parser.LineScanner{bufio.NewScanner(file), 0}

	for {
		entry, err := parseLogEntry(&scanner)
		if err == io.EOF {
			return entries, nil
		} else if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
}

func parseLogEntry(scanner *parser.LineScanner) (*ResultLogEntry, error) {
	entry := &ResultLogEntry{}

	var err error
	linesScanned := 0
	for scanner.Scan() {
		line := scanner.Text()
		linesScanned++

		// Sample line:
		// 2019-10-16T12:20:29.0594292-07:00 index/random/10/slt_good_0.test:535: SELECT * FROM tab0 AS cor0 WHERE NULL <> 29 + col0 not ok: Schemas differ. Expected IIIIIII, got IIRTIRT
		firstSpace := strings.Index(line," ")
		if firstSpace == -1 {
			// unrecognized log line, ignore and continue
			continue
		}

		entry.entryTime, err = time.Parse(time.RFC3339Nano, line[:firstSpace])
		if err != nil {
			// unrecognized log line, ignore and continue
			continue
		}

		if strings.HasSuffix(line, "ok") {
			entry.result = Ok
		} else if strings.Contains(line, "not ok:") {
			entry.result = NotOk
		} else if strings.HasSuffix(line, "skipped") {
			entry.result = Skipped
		} else {
			panic("Couldn't determine result of log line " + line)
		}

		colonIdx := strings.Index(line[firstSpace+1:], ":")
		if colonIdx == -1 {
			panic(fmt.Sprintf("Malformed line %v on line %d", line, scanner.LineNum))
		} else {
			colonIdx = colonIdx + firstSpace + 1
		}

		entry.testFile = line[firstSpace+1:colonIdx]
		colonIdx2 := strings.Index(line[colonIdx+1:], ":")
		if colonIdx2 == -1 {
			panic(fmt.Sprintf("Malformed line %v on line %d", line, scanner.LineNum))
		} else {
			colonIdx2 = colonIdx + 1 + colonIdx2
		}

		entry.lineNum, err = strconv.Atoi(line[colonIdx+1:colonIdx2])
		if err != nil {
			panic(fmt.Sprintf("Failed to parse line number on line %v", scanner.LineNum))
		}

		switch entry.result {
		case NotOk:
			eoq := strings.Index(line[colonIdx2+1:], "not ok: ") + colonIdx2+1
			entry.query = line[colonIdx2+2:eoq-1]
			entry.errorMessage = line[eoq+len("not ok: "):]
		case Ok:
			eoq := strings.Index(line[colonIdx2+1:], "ok") + colonIdx2+1
			entry.query = line[colonIdx2+2:eoq-1]
		case Skipped:
			eoq := strings.Index(line[colonIdx2+1:], "skipped") + colonIdx2+1
			entry.query = line[colonIdx2+2:eoq-1]
		}

		return entry, nil
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	if scanner.Err() == nil && linesScanned == 0 {
		return nil, io.EOF
	}

	return entry, nil
}