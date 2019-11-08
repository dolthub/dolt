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

package tests

import (
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestDatetimeQueries(t *testing.T) {
	tests := []struct {
		inputSQLVal interface{}
		inputValue  types.Timestamp
	}{
		{"2020-10-07 06:24:11.472294", types.Timestamp(time.Date(2020, 10, 7, 6, 24, 11, 472294000, time.UTC))},
		{"1000-01-01 00:00:00", types.Timestamp(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"9999-12-31 23:59:59", types.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC))},
		{"1970-01-01 00:00:00", types.Timestamp(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"1610-06-08 09:10:37", types.Timestamp(time.Date(1610, 6, 8, 9, 10, 37, 0, time.UTC))},
		{"1945-05-08 19:33:41", types.Timestamp(time.Date(1945, 5, 8, 19, 33, 41, 0, time.UTC))},
		{"3005-02-14 10:02:25", types.Timestamp(time.Date(3005, 2, 14, 10, 02, 25, 0, time.UTC))},
		{"2019-11-01 03:23:48", types.Timestamp(time.Date(2019, 11, 1, 3, 23, 48, 0, time.UTC))},
		{"1000-01-01", types.Timestamp(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"9999-12-31", types.Timestamp(time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC))},
		{"1970-01-01", types.Timestamp(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))},
		{"1610-06-08", types.Timestamp(time.Date(1610, 6, 8, 0, 0, 0, 0, time.UTC))},
		{"1945-05-08", types.Timestamp(time.Date(1945, 5, 8, 0, 0, 0, 0, time.UTC))},
		{"3005-02-14", types.Timestamp(time.Date(3005, 2, 14, 0, 0, 0, 0, time.UTC))},
		{"2019-11-01", types.Timestamp(time.Date(2019, 11, 1, 0, 0, 0, 0, time.UTC))},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.inputSQLVal), func(t *testing.T) {
			testParse(t, test.inputSQLVal, test.inputValue, sql.Datetime)
		})
	}
}

func TestDatetimeRoundtrip(t *testing.T) {
	tests := []string{
		"1000-01-01 00:00:00",
		"9999-12-31 23:59:59",
		"1970-01-01 00:00:00",
		"1610-06-08 09:10:37",
		"1945-05-08 19:33:41",
		"3005-02-14 10:02:25",
		"2019-11-01 03:23:48",
		"2020-10-07 06:24:11.472294",
	}

	conn, serverController := runServer(t)
	defer closeServer(t, conn, serverController)
	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			roundTrip(t, test, sql.Datetime, conn)
		})
	}
}
