// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build freebsd

package unix_test

import (
	"os"
	"testing"

	"gx/ipfs/QmTq8ag5pgTCqtGDtmpm1F5TPE2i1H8bcU6295WFKTc5ie/sys/unix"
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
