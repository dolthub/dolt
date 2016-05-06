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
