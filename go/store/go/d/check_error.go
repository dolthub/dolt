// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package d

import (
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/util/exit"
	flag "github.com/juju/gnuflag"
)

func CheckError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		flag.Usage()
		exit.Fail()
	}
}

func CheckErrorNoUsage(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		exit.Fail()
	}
}
