// Copyright 2023 Dolthub, Inc.
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

package dconfig

import (
	"time"

	errors2 "gopkg.in/src-d/go-errors.v1"
)

// SupportedLayouts is the set of time string formats for configuration date strings.
// We are more permissive than what is documented.
var SupportedLayouts = []string{
	"2006/01/02",
	"2006/01/02T15:04:05",
	"2006/01/02T15:04:05Z07:00",

	"2006.01.02",
	"2006.01.02T15:04:05",
	"2006.01.02T15:04:05Z07:00",

	"2006-01-02",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
}

var (
	ErrUnsupportedDateFormat = errors2.NewKind("error: '%s' is not in a supported format.")
)

// ParseDate attempt to parse a date string into a time.Time object.
func ParseDate(dateStr string) (time.Time, error) {
	for _, layout := range SupportedLayouts {
		t, err := time.Parse(layout, dateStr)

		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, ErrUnsupportedDateFormat.New(dateStr)
}
