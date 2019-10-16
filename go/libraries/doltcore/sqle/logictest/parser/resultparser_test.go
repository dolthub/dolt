package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseResultFile(t *testing.T) {
	entries, err := ParseResultFile("testdata/resultlog.txt")
	assert.NoError(t, err)

	expectedResults := []*ResultLogEntry {
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3408696-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      25,
			query:        "SELECT 1 IN ()",
			result:       Skipped,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      30,
			query:        "SELECT 1 IN (2)",
			result:       Ok,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      35,
			query:        "SELECT 1 IN (2,3,4,5,6,7,8,9)",
			result:       Ok,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      41,
			query:        "SELECT 1 NOT IN ()",
			result:       Skipped,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      46,
			query:        "SELECT 1 NOT IN (2)",
			result:       Ok,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      51,
			query:        "SELECT 1 NOT IN (2,3,4,5,6,7,8,9)",
			result:       Ok,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      57,
			query:        "SELECT null IN ()",
			result:       Skipped,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      63,
			query:        "SELECT null NOT IN ()",
			result:       Skipped,
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3428692-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      68,
			query:        "CREATE TABLE t1(x INTEGER)",
			result:       NotOk,
			errorMessage: "Unexpected error no primary key columns",
		},
		{
			entryTime:    mustParseTime("2019-10-16T16:02:18.3428692-07:00"),
			testFile:     "evidence/in1.test",
			lineNum:      72,
			query:        "SELECT 1 IN t1",
			result:       Skipped,
		},
	}

	assert.Equal(t, expectedResults, entries)
}

func mustParseTime(t string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return parsed
}