package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		usageError()
	}
	d, err := time.ParseDuration(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not parse duration: %s\n", err)
		usageError()
	}

	time.Sleep(d)
}

func usageError() {
	fmt.Fprintf(os.Stderr, "Usage: %s <duration>\n", os.Args[0])
	fmt.Fprintln(os.Stderr, `Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".`)
	fmt.Fprintln(os.Stderr, "See https://godoc.org/time#ParseDuration for more.")
	os.Exit(-1)
}
