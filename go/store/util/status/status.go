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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package status prints status messages to a console, overwriting previous values.
package status

import (
	"fmt"
	"time"
)

const (
	clearLine = "\x1b[2K\r"
	Rate      = 100 * time.Millisecond
)

var (
	lastTime   time.Time
	lastFormat string
	lastArgs   []interface{}
)

func Clear() {
	fmt.Print(clearLine)
	reset(time.Time{})
}

func WillPrint() bool {
	return time.Since(lastTime) >= Rate
}

func Printf(format string, args ...interface{}) {
	now := time.Now()
	if now.Sub(lastTime) < Rate {
		lastFormat, lastArgs = format, args
	} else {
		fmt.Printf(clearLine+format, args...)
		reset(now)
	}
}

func Done() {
	if lastArgs != nil {
		fmt.Printf(clearLine+lastFormat, lastArgs...)
	}
	fmt.Println()
	reset(time.Time{})
}

func reset(time time.Time) {
	lastTime = time
	lastFormat, lastArgs = "", nil
}
