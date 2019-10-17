package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFile(t *testing.T) {
	f := "testdata/select1.test"
	records, err := ParseTestFile(f)
	require.NoError(t, err)

	expectedRecords := []*Record{
		{
			recordType:  Statement,
			expectError: false,
			query:       "CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)",
			lineNum:     2,
		},
		{
			recordType:  Statement,
			expectError: false,
			query:       "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)",
			lineNum:     5,
		},
		{
			recordType:  Statement,
			expectError: true,
			query:       "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)",
			lineNum:     8,
		},
		{
			recordType: Halt,
			lineNum:    11,
		},
		{
			recordType: Query,
			schema:     "I",
			sortMode:   "nosort",
			query: removeNewlines(`SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1`),
			result:  []string{"30 values hashing to 3c13dee48d9356ae19af2515e05e6b54"},
			lineNum: 14,
		},
		{
			recordType: Query,
			schema:     "II",
			sortMode:   "nosort",
			label:      "label-1",
			query: removeNewlines(`SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 ORDER BY 1,2`),
			result:  []string{"60 values hashing to 808146289313018fce25f1a280bd8c30"},
			lineNum: 29,
		},
		{
			recordType: Halt,
			condition: &Condition{
				isOnly: true,
				engine: "mysql",
			},
			lineNum: 37,
		},
		{
			recordType: Query,
			schema:     "IIIII",
			sortMode:   "rowsort",
			query: removeNewlines(`SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1
 WHERE (e>c OR e<d)
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
 ORDER BY 4,2,1,3,5`),
			condition: &Condition{
				isOnly: true,
				engine: "mysql",
			},
			result:  []string{"1", "2", "3", "4", "5"},
			lineNum: 41,
		},
		{
			recordType: Query,
			schema:     "II",
			sortMode:   "nosort",
			query: removeNewlines(`SELECT a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c>d
   AND b>c
 ORDER BY 2,1`),
			condition: &Condition{
				isSkip: true,
				engine: "mssql",
			},
			result:  []string{"-3", "222", "-3", "222", "-1", "222", "-1", "222"},
			lineNum: 62,
		},
	}

	assert.Equal(t, expectedRecords, records)
}

func removeNewlines(s string) string {
	return strings.ReplaceAll(s, "\n", "")
}
