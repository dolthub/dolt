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

package types

import (
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	"github.com/src-d/go-mysql-server/sql"

	dtypes "github.com/liquidata-inc/dolt/go/store/types"
)

type datetimeType struct{}

func (datetimeType) NomsKind() dtypes.NomsKind {
	return dtypes.TimestampKind
}

func (datetimeType) SqlType() sql.Type {
	return sql.Datetime
}

func (datetimeType) SqlTypes() []sql.Type {
	return []sql.Type{sql.Date, sql.Datetime, sql.Timestamp}
}

func (datetimeType) GetValueToSql() ValueToSql {
	return func(val dtypes.Value) (interface{}, error) {
		if v, ok := val.(dtypes.Timestamp); ok {
			return time.Time(v), nil
		}
		return nil, fmt.Errorf("expected Timestamp, recevied %v", val.Kind())
	}
}

func (datetimeType) GetSqlToValue() SqlToValue {
	return func(val interface{}) (dtypes.Value, error) {
		switch e := val.(type) {
		case string:
			t, err := dateparse.ParseStrict(e)
			if err != nil {
				return nil, err
			}
			return dtypes.Timestamp(t), nil
		default:
			return nil, fmt.Errorf("cannot convert SQL type <%T> val <%v> to Timestamp", val, val)
		}
	}
}

func (datetimeType) String() string {
	return "datetimeType"
}
