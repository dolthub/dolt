// Package status prints status messages to a console, overwriting previous values.
package status

import (
	"fmt"
)

const (
	clearLine = "\x1b[2K\r"
)

func Clear() {
	fmt.Print(clearLine)
}

func Printf(format string, args ...interface{}) {
	// Can't call Clear() here because it causes flicker
	fmt.Printf(clearLine+format, args...)
}

func Done() {
	fmt.Println("")
}
