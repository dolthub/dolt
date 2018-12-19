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
	return time.Now().Sub(lastTime) >= Rate
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
