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

package dolttypes

import (
	"fmt"
	"github.com/araddon/dateparse"
	"time"
)

const timeFormat = "Mon Jan 2 15:04:05 -0700"

type Timestamp time.Time

func (v Timestamp) Compare(other DoltType) int {
	v1 := time.Time(v)
	v2 := time.Time(other.(Timestamp))
	if v1.Before(v2) {
		return -1
	} else if v1.Equal(v2) {
		return 0
	}
	return 1
}

func (v Timestamp) Decode(data []byte) (DoltType, error) {
	t := time.Time{}
	err := t.UnmarshalBinary(data[DoltKindLength:])
	if err != nil {
		return nil, err
	}
	return Timestamp(t), nil
}

func (v Timestamp) Encode() ([]byte, error) {
	t := time.Time(v)
	data, err := t.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return TimestampKind.PrependKind(data), nil
}

func (v Timestamp) Equals(other DoltType) bool {
	v1 := time.Time(v)
	v2 := time.Time(other.(Timestamp))
	return v1.Equal(v2)
}

func (v Timestamp) Kind() DoltKind {
	return TimestampKind
}

func (v Timestamp) MarshalBool() (bool, error) {
	return false, fmt.Errorf("cannot serialize timestamp to bool")
}

func (v Timestamp) MarshalDoltType(kind DoltKind) (DoltType, error) {
	switch kind {
	case TimestampKind:
		return v, nil
	default:
		return nil, fmt.Errorf("cannot serialize timestamp to %v", kind.String())
	}
}

func (v Timestamp) MarshalFloat() (float64, error) {
	t := time.Time(v)
	seconds := t.Unix()
	// Since Float allows decimals, we represent the nanoseconds as a decimal
	nanoseconds := t.Nanosecond()
	combination := float64(seconds) + (float64(nanoseconds) / float64(time.Second / time.Nanosecond))
	return combination, nil
}

func (v Timestamp) MarshalInt() (int64, error) {
	return time.Time(v).Unix(), nil
}

func (v Timestamp) MarshalString() (string, error) {
	return v.String(), nil
}

func (v Timestamp) MarshalUint() (uint64, error) {
	return uint64(time.Time(v).Unix()), nil
}

func (v Timestamp) UnmarshalBool(bool) (DoltType, error) {
	return nil, fmt.Errorf("cannot deserialize bool to timestamp")
}

func (v Timestamp) UnmarshalDoltType(dt DoltType) (DoltType, error) {
	switch dt.Kind() {
	case TimestampKind:
		return dt, nil
	default:
		return v, fmt.Errorf("cannot deserialize %v to timestamp", dt.Kind())
	}
}

func (v Timestamp) UnmarshalFloat(fl float64) (DoltType, error) {
	// If Float is too large, we'll clamp it to the max time representable
	// The usable max is calculated by taking the int64 max (1<<63-1) and subtracting time.unixToInternal, which is not exposed
	if fl > 1<<63-62135596801 {
		fl = 1<<63-62135596801
		// I could not find anything pointing to a minimum allowed time, so "-200000000-01-01 00:00:00" seems reasonable
	} else if fl < -6311452567219200 {
		fl = -6311452567219200
	}
	// We treat a Float as seconds and nanoseconds, unlike integers which are just seconds
	seconds := int64(fl)
	nanoseconds := int64((fl-float64(seconds)) * float64(time.Second / time.Nanosecond))
	return Timestamp(time.Unix(seconds, nanoseconds).UTC()), nil
}

func (v Timestamp) UnmarshalInt(n int64) (DoltType, error) {
	// If Int is too large, we'll clamp it to the max time representable
	// The usable max is calculated by taking the int64 max (1<<63-1) and subtracting time.unixToInternal, which is not exposed
	if n > 1<<63-62135596801 {
		n = 1<<63-62135596801
		// I could not find anything pointing to a minimum allowed time, so "-200000000-01-01 00:00:00" seems reasonable
	} else if n < -6311452567219200 {
		n = -6311452567219200
	}
	return Timestamp(time.Unix(n, 0).UTC()), nil
}

func (v Timestamp) UnmarshalString(s string) (DoltType, error) {
	t, err := dateparse.ParseStrict(s)
	if err != nil {
		return nil, err
	}
	return Timestamp(t), nil
}

func (v Timestamp) UnmarshalUint(n uint64) (DoltType, error) {
	// If Uint is too large, we'll clamp it to the max time representable
	// The usable max is calculated by taking the int64 max (1<<63-1) and subtracting time.unixToInternal, which is not exposed
	if n > 1<<63-62135596801 {
		n = 1<<63-62135596801
	}
	return Timestamp(time.Unix(int64(n), 0).UTC()), nil
}

func (v Timestamp) String() string {
	return time.Time(v).Format(timeFormat)
}