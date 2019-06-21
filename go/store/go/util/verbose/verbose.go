// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package verbose

import (
	"log"

	flag "github.com/juju/gnuflag"
)

var (
	verbose bool
	quiet   bool
)

// RegisterVerboseFlags registers -v|--verbose flags for general usage
func RegisterVerboseFlags(flags *flag.FlagSet) {
	flags.BoolVar(&verbose, "verbose", false, "show more")
	flags.BoolVar(&verbose, "v", false, "")
	flags.BoolVar(&quiet, "quiet", false, "show less")
	flags.BoolVar(&quiet, "q", false, "")
}

// Verbose returns True if the verbose flag was set
func Verbose() bool {
	return verbose
}

func SetVerbose(v bool) {
	verbose = v
}

// Quiet returns True if the verbose flag was set
func Quiet() bool {
	return quiet
}

func SetQuiet(q bool) {
	quiet = q
}

// Log calls Printf(format, args...) iff Verbose() returns true.
func Log(format string, args ...interface{}) {
	if Verbose() {
		if len(args) > 0 {
			log.Printf(format+"\n", args...)
		} else {
			log.Println(format)
		}
	}
}
