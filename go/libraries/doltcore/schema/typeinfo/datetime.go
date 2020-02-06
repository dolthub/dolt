// Copyright 2020 Liquidata, Inc.
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

package typeinfo

import (
	"fmt"
	"strconv"
	"time"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	datetimeTypeParam_Min      = "min"
	datetimeTypeParam_MinNano  = "minnano"
	datetimeTypeParam_Max      = "max"
	datetimeTypeParam_MaxNano  = "maxnano"
	datetimeTypeParam_DateOnly = "date"
)

type datetimeImpl struct {
	Min      time.Time
	Max      time.Time
	DateOnly bool
}

var _ TypeInfo = (*datetimeImpl)(nil)

func CreateDatetimeTypeFromParams(params map[string]string) (TypeInfo, error) {
	var minInt int64
	var minNanoInt int64
	var err error
	if minStr, ok := params[datetimeTypeParam_Min]; ok {
		minInt, err = strconv.ParseInt(minStr, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create datetime type info is missing param "%v"`, datetimeTypeParam_Min)
	}
	if minNanoStr, ok := params[datetimeTypeParam_MinNano]; ok {
		minNanoInt, err = strconv.ParseInt(minNanoStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	var maxInt int64
	var maxNanoInt int64
	if maxStr, ok := params[datetimeTypeParam_Max]; ok {
		maxInt, err = strconv.ParseInt(maxStr, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf(`create datetime type info is missing param "%v"`, datetimeTypeParam_Max)
	}
	if maxNanoStr, ok := params[datetimeTypeParam_MaxNano]; ok {
		maxNanoInt, err = strconv.ParseInt(maxNanoStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	var dateOnly bool
	if _, ok := params[datetimeTypeParam_DateOnly]; ok {
		dateOnly = true
	}
	ti := &datetimeImpl{time.Unix(minInt, minNanoInt), time.Unix(maxInt, maxNanoInt), dateOnly}
	if dateOnly {
		ti.Min = ti.Min.Truncate(24 * time.Hour)
		ti.Max = ti.Max.Truncate(24 * time.Hour)
	}
	if ti.Min.After(ti.Max) || ti.Min.Equal(ti.Max) {
		return nil, fmt.Errorf("create datetime type info has min >= max which is disallowed")
	}
	return ti, nil
}

// ConvertNomsValueToValue implements TypeInfo interface.
func (ti *datetimeImpl) ConvertNomsValueToValue(v types.Value) (interface{}, error) {
	//TODO: handle the zero value as a special case that is valid for all ranges
	if val, ok := v.(types.Timestamp); ok {
		t := time.Time(val).UTC()
		if ti.DateOnly {
			t = t.Truncate(24 * time.Hour)
		}
		if (t.After(ti.Min) && t.Before(ti.Max)) || t.Equal(ti.Min) || t.Equal(ti.Max) {
			return t, nil
		}
		return nil, fmt.Errorf(`"%v" cannot convert time "%v" to value`, ti.String(), t.String())
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return nil, nil
	}
	return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), v.Kind())
}

// ConvertValueToNomsValue implements TypeInfo interface.
func (ti *datetimeImpl) ConvertValueToNomsValue(v interface{}) (types.Value, error) {
	//TODO: handle the zero value as a special case that is valid for all ranges
	if artifact, ok := ti.isValid(v); ok {
		switch val := v.(type) {
		case nil:
			return types.NullValue, nil
		case string:
			return types.Timestamp(artifact), nil
		case types.Null:
			return types.NullValue, nil
		case time.Time:
			return types.Timestamp(artifact), nil
		case types.String:
			return types.Timestamp(artifact), nil
		case types.Timestamp:
			return types.Timestamp(artifact), nil
		default:
			return nil, fmt.Errorf(`"%v" has falsely evaluated value "%v" of type "%T" as valid`, ti.String(), val, val)
		}
	}
	return nil, fmt.Errorf(`"%v" cannot convert value "%v" of type "%T" as it is invalid`, ti.String(), v, v)
}

// Equals implements TypeInfo interface.
func (ti *datetimeImpl) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if ti2, ok := other.(*datetimeImpl); ok {
		return ti.Min.Equal(ti2.Min) && ti.Max.Equal(ti2.Max)
	}
	return false
}

// GetTypeIdentifier implements TypeInfo interface.
func (ti *datetimeImpl) GetTypeIdentifier() Identifier {
	return DatetimeTypeIdentifier
}

// GetTypeParams implements TypeInfo interface.
func (ti *datetimeImpl) GetTypeParams() map[string]string {
	params := map[string]string{
		datetimeTypeParam_Min: strconv.FormatInt(ti.Min.Unix(), 10),
		datetimeTypeParam_Max: strconv.FormatInt(ti.Max.Unix(), 10),
	}
	if ti.DateOnly {
		params[datetimeTypeParam_DateOnly] = ""
	}
	return params
}

// IsValid implements TypeInfo interface.
func (ti *datetimeImpl) IsValid(v interface{}) bool {
	_, ok := ti.isValid(v)
	return ok
}

// NomsKind implements TypeInfo interface.
func (ti *datetimeImpl) NomsKind() types.NomsKind {
	return types.TimestampKind
}

// String implements TypeInfo interface.
func (ti *datetimeImpl) String() string {
	dateOnly := ""
	if ti.DateOnly {
		dateOnly = ", DateOnly"
	}
	return fmt.Sprintf(`Datetime(Min: "%v", Max: "%v"%v)`, ti.Min.String(), ti.Max.String(), dateOnly)
}

// ToSqlType implements TypeInfo interface.
func (ti *datetimeImpl) ToSqlType() sql.Type {
	if ti.DateOnly {
		return sql.Date
	}
	minTimestamp := sql.Timestamp.MinimumTime()
	maxTimestamp := sql.Timestamp.MaximumTime()
	if (ti.Min.Equal(minTimestamp) || ti.Min.After(minTimestamp)) && (ti.Max.Equal(maxTimestamp) || ti.Max.Before(maxTimestamp)) {
		return sql.Timestamp
	}
	return sql.Datetime
}

// isValid is an internal implementation for the TypeInfo interface function IsValid.
// Some validity checks process the value into its final form, which may be returned
// as an artifact so that a value doesn't need to be processed twice in some scenarios.
func (ti *datetimeImpl) isValid(v interface{}) (artifact time.Time, ok bool) {
	//TODO: handle the zero value as a special case that is valid for all ranges
	switch val := v.(type) {
	case nil:
		return time.Time{}, true
	case string:
		for _, format := range sql.TimestampDatetimeLayouts {
			if t, err := time.Parse(format, val); err == nil {
				t = t.UTC()
				if ti.DateOnly {
					t = t.Truncate(24 * time.Hour)
				}
				if (t.After(ti.Min) && t.Before(ti.Max)) || t.Equal(ti.Min) || t.Equal(ti.Max) {
					return t, true
				}
				return time.Time{}, false
			}
		}
		return time.Time{}, false
	case types.Null:
		return time.Time{}, true
	case time.Time:
		val = val.UTC()
		if ti.DateOnly {
			val = val.Truncate(24 * time.Hour)
		}
		if (val.After(ti.Min) && val.Before(ti.Max)) || val.Equal(ti.Min) || val.Equal(ti.Max) {
			return val, true
		}
		return time.Time{}, false
	case types.String:
		valStr := string(val)
		for _, format := range sql.TimestampDatetimeLayouts {
			if t, err := time.Parse(format, valStr); err == nil {
				t = t.UTC()
				if ti.DateOnly {
					t = t.Truncate(24 * time.Hour)
				}
				if (t.After(ti.Min) && t.Before(ti.Max)) || t.Equal(ti.Min) || t.Equal(ti.Max) {
					return t, true
				}
				return time.Time{}, false
			}
		}
		return time.Time{}, false
	case types.Timestamp:
		t := time.Time(val).UTC()
		if ti.DateOnly {
			t = t.Truncate(24 * time.Hour)
		}
		if (t.After(ti.Min) && t.Before(ti.Max)) || t.Equal(ti.Min) || t.Equal(ti.Max) {
			return t, true
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}
