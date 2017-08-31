// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin

package fsnotify

import "gx/ipfs/QmTsTrxKNXu8sZLv7FP6p884CHoDbT9upKA1j4FkCy5V7T/sys/unix"

// note: this constant is not defined on BSD
const openMode = unix.O_EVTONLY
