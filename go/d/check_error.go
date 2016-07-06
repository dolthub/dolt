// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package d

import (
	"flag"
	"fmt"
	"os"
)

type Exiter interface {
	Exit(code int)
}

type nomsExiter struct{}

func (e nomsExiter) Exit(code int) {
	os.Exit(code)
}

var UtilExiter Exiter = nomsExiter{}

func CheckError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		flag.Usage()
		UtilExiter.Exit(-1)
	}
}

func CheckErrorNoUsage(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		UtilExiter.Exit(-1)
	}
}
