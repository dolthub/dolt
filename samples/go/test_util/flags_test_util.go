// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package test_util

import (
    "fmt"
)

func CreateDatabaseSpecString(protocol, path string) string {
    return fmt.Sprintf("%s:%s", protocol, path)
}

func CreateValueSpecString(protocol, path, value string) string {
    return fmt.Sprintf("%s:%s::%s", protocol, path, value)
}
