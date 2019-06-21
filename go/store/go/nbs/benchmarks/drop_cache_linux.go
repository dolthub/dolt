// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// +build linux

package main

import "os/exec"

func dropCache() error {
	return exec.Command("./drop_cache").Run()
}
