// Copyright 2022 Dolthub, Inc.
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

package tabular

import (
	"context"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
)

func TestFixedWidthWriter(t *testing.T) {
	sch := sql.Schema{
		{Name: nameColName, Type: sql.Text},
		{Name: ageColName, Type: sql.Text},
		{Name: titleColName, Type: sql.Text},
	}

	names := []interface{}{
		"Michael Scott",
		"Pam Beasley",
		"Dwight Schrute",
		"Jim Halpêrt",
	}
	ages := []interface{}{
		"43",
		"25",
		"29",
		nil,
	}
	titles := []interface{}{
		"Regional Manager",
		"Secretary",
		"Assistant to the Regional Manager",
		nil,
	}

	rows := make([]sql.Row, len(ages))
	for i := range ages {
		rows[i] = sql.Row{names[i], ages[i], titles[i]}
	}

	t.Run("Column names bigger than row data", func(t *testing.T) {
		var stringWr StringBuilderCloser
		biggerSch := sch.Copy()
		for _, col := range biggerSch {
			col.Name = col.Name + " a really long string for this name"
		}
		tableWr := NewFixedWidthTableWriter(biggerSch, &stringWr, 100)

		var expectedTableString = `
+-----------------------------------------+----------------------------------------+------------------------------------------+
| name a really long string for this name | age a really long string for this name | title a really long string for this name |
+-----------------------------------------+----------------------------------------+------------------------------------------+
| Michael Scott                           | 43                                     | Regional Manager                         |
| Pam Beasley                             | 25                                     | Secretary                                |
| Dwight Schrute                          | 29                                     | Assistant to the Regional Manager        |
| Jim Halpêrt                             | NULL                                   | NULL                                     |
+-----------------------------------------+----------------------------------------+------------------------------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			err := tableWr.WriteRow(context.Background(), r, nil)
			assert.NoError(t, err)
		}

		err := tableWr.Close(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, expectedTableString, stringWr.String())
	})

	t.Run("Sample size bigger than num rows", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewFixedWidthTableWriter(sch, &stringWr, 100)

		var expectedTableString = `
+----------------+------+-----------------------------------+
| name           | age  | title                             |
+----------------+------+-----------------------------------+
| Michael Scott  | 43   | Regional Manager                  |
| Pam Beasley    | 25   | Secretary                         |
| Dwight Schrute | 29   | Assistant to the Regional Manager |
| Jim Halpêrt    | NULL | NULL                              |
+----------------+------+-----------------------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			err := tableWr.WriteRow(context.Background(), r, nil)
			assert.NoError(t, err)
		}

		err := tableWr.Close(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, expectedTableString, stringWr.String())
	})

	t.Run("Sample size smaller than num rows, overflow", func(t *testing.T) {
		var stringWr StringBuilderCloser
		tableWr := NewFixedWidthTableWriter(sch, &stringWr, 2)

		var expectedTableString = `
+---------------+-----+------------------+
| name          | age | title            |
+---------------+-----+------------------+
| Michael Scott | 43  | Regional Manager |
| Pam Beasley   | 25  | Secretary        |
| Dwight Schrute | 29  | Assistant to the Regional Manager |
| Jim Halpêrt   | NULL | NULL             |
+---------------+-----+------------------+
`
		// strip off the first newline, inserted for nice printing
		expectedTableString = strings.Replace(expectedTableString, "\n", "", 1)

		for _, r := range rows {
			err := tableWr.WriteRow(context.Background(), r, nil)
			assert.NoError(t, err)
		}

		err := tableWr.Close(context.Background())
		assert.NoError(t, err)

		assert.Equal(t, expectedTableString, stringWr.String())
	})
}
