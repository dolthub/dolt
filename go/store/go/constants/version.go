// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package constants collects common constants used in Noms, such as the Noms data format version.
package constants

import (
	"fmt"
	"os"
)

const NomsVersion = "7.18"
const NOMS_VERSION_NEXT_ENV_NAME = "NOMS_VERSION_NEXT"
const NOMS_VERSION_NEXT_ENV_VALUE = "1"

var NomsGitSHA = "<developer build>"

func init() {
	if os.Getenv(NOMS_VERSION_NEXT_ENV_NAME) != NOMS_VERSION_NEXT_ENV_VALUE {
		fmt.Fprintln(os.Stderr,
			"WARNING: This is an unstable version of Noms. Data created with it won't be supported.")
		fmt.Fprintf(os.Stderr,
			"Please see %s for getting the latest supported version.\n",
			"https://github.com/attic-labs/noms#install-noms")
		fmt.Fprintf(os.Stderr,
			"Or add %s=%s to your environment to proceed with this version.\n",
			NOMS_VERSION_NEXT_ENV_NAME,
			NOMS_VERSION_NEXT_ENV_VALUE)
		os.Exit(1)
	}
}
