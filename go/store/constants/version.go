// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package constants collects common constants used in Noms, such as the Noms data format version.
package constants

import "os"

var DefaultNomsBinFormat = "7.18"

func init() {
	nbf := os.Getenv("DOLT_DEFAULT_BIN_FORMAT")
	if nbf != "" {
		DefaultNomsBinFormat = nbf
	}
}

const NomsVersion = "7.18"

var NomsGitSHA = "<developer build>"
