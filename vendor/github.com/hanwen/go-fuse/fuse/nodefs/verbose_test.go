// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nodefs

import "flag"

// VerboseTest returns true if the testing framework is run with -v.
func VerboseTest() bool {
	flag := flag.Lookup("test.v")
	return flag != nil && flag.Value.String() == "true"
}
