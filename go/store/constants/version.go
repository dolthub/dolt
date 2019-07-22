// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package constants collects common constants used in Noms, such as the Noms data format version.
package constants

import "os"

func init() {
	nbfVerStr := os.Getenv("DOLT_DEFAULT_BIN_FORMAT")
	if nbfVerStr != "" {
		FormatDefaultString = nbfVerStr
	}
}

const NomsVersion = "7.18"

var NomsGitSHA = "<developer build>"

// See //go/store/types/format.go for corresponding formats.

const Format718String = "7.18"
const FormatLD1String = "__LD_1__"

var FormatDefaultString = Format718String
