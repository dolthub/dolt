// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build freebsd

package unix_test

import (
	"os"
	"testing"

	"gx/ipfs/QmTsTrxKNXu8sZLv7FP6p884CHoDbT9upKA1j4FkCy5V7T/sys/unix"
)

func TestSysctUint64(t *testing.T) {
	_, err := unix.SysctlUint64("vm.max_kernel_address")
	if err != nil {
		if os.Getenv("GO_BUILDER_NAME") == "freebsd-386-gce101" {
			t.Skipf("Ignoring known failing test (golang.org/issue/15186). Failed with: %v", err)
		}
		t.Fatal(err)
	}
}
