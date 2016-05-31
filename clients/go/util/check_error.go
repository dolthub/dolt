// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package util

import (
    "flag"
    "fmt"
    "os"
)

func CheckError(err error) {
    if err != nil {
        fmt.Fprintf(os.Stderr, "error: %s\n", err)
        flag.Usage()
        os.Exit(-1)
    }
}
