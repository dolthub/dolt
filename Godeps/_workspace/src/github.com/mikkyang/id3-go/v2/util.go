// Copyright 2013 Michael Yang. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package v2

func isBitSet(flag, index byte) bool {
	return flag&(1<<index) != 0
}
