// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sort

import "testing"

// todo test merging (two files combine to one with expected size and same values on read)
// todo test compact (2*n files compact to n, size doubles, reads back to same values)
// todo test sort (make sure comparison works correctly)
// todo test mem iter (keys in memory all come back)
// todo test file round-trip (flush mem, read back through file iter)

// helpers -> tempdir provider, tuples and sort comparison

func TestMemSort(t *testing.T) {

}

func TestMemIter(t *testing.T) {

}

func TestMemMerge(t *testing.T) {

}

func TestMemCompact(t *testing.T) {

}

func TestFileRoundtrip(t *testing.T) {

}

func TestFileMerge(t *testing.T) {

}

func TestCompact(t *testing.T) {

}

func TestFileCompact(t *testing.T) {

}

func TestFileE2E(t *testing.T) {
	// make the batch size and file size small enough that
	// we have to spill to disk and compact several times
}
