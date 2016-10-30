// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nodefs

import "time"

func splitDuration(dt time.Duration, secs *uint64, nsecs *uint32) {
	ns := int64(dt)
	*nsecs = uint32(ns % 1e9)
	*secs = uint64(ns / 1e9)
}
