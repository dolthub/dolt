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

package engine

import (
	"bytes"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSecondsSince(t *testing.T) {
	t.Run("1 second passes", func(t *testing.T) {
		start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		stop := time.Date(2022, 1, 1, 0, 0, 1, 0, time.UTC)
		require.Equal(t, 1.0, secondsSince(start, stop))
	})
	t.Run("1 second and 1 millisecond passes", func(t *testing.T) {
		start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		stop := time.Date(2022, 1, 1, 0, 0, 1, int(1*time.Millisecond), time.UTC)
		require.Equal(t, 1.001, secondsSince(start, stop))
	})
	t.Run("1 second and 0.5 millisecond passes", func(t *testing.T) {
		start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		stop := time.Date(2022, 1, 1, 0, 0, 1, int(1*time.Millisecond/2), time.UTC)
		require.Equal(t, 1.000, secondsSince(start, stop))
	})
}

func TestJSONLResults(t *testing.T) {
	sch := sql.Schema{&sql.Column{Name: "id", Type: types.Int64}, &sql.Column{Name: "value", Type: types.LongText}}
	for _, tt := range []struct {
		name string
		rows []sql.Row
		want string
	}{
		{"empty", nil, ""},
		{"one row", []sql.Row{{int64(1), "text"}}, "{\"id\":1,\"value\":\"text\"}\n"},
		{"multiple rows and escaped newline", []sql.Row{{int64(1), "a\nb"}, {int64(2), nil}}, "{\"id\":1,\"value\":\"a\\nb\"}\n{\"id\":2}\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			old := cli.CliOut
			cli.CliOut = &buf
			defer func() { cli.CliOut = old }()
			err := PrettyPrintResults(sql.NewEmptyContext(), FormatJsonl, sch, sql.RowsToRowIter(tt.rows...), false, false, false, false)
			require.NoError(t, err)
			require.Equal(t, tt.want, buf.String())
		})
	}
}
