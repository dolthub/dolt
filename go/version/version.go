// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package version contains utilities for working with the Noms format version.
package version

import (
	flag "github.com/juju/gnuflag"
)

var (
	// TODO: generate this from some central thing with go generate, so that JS and Go can be easily kept in sync
	nomsVersionStable = "7"
	nomsVersionNext   = "8"
	NomsGitSHA        = "<developer build>"
	useVersionNext    = false
)

func RegisterVersionFlags(flags flag.FlagSet) {
	flags.BoolVar(&useVersionNext, "v8", false, "Enables noms format version 8")
}

func Current() string {
	if useVersionNext {
		return nomsVersionNext
	} else {
		return nomsVersionStable
	}
}

func IsStable() bool {
	return Current() == nomsVersionStable
}

func IsNext() bool {
	return Current() == nomsVersionNext
}

func UseNext(v bool) {
	useVersionNext = v
}
