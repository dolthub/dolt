// Copyright 2026 Dolthub, Inc.
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

//go:build linux

package nbs

import (
	"os"

	"golang.org/x/sys/unix"
)

// journalPrepareStep is how far past a write the journal is zero-filled.
// Writing into blocks that already hold data lets fdatasync skip the
// filesystem metadata commit that appending (or writing into sparse or
// fallocate'd space) would require on every journal sync.
const journalPrepareStep = 4 << 20

// syncFileData makes the file's data durable. fdatasync flushes the data and
// any metadata needed to read it back (size, block allocation), but not
// timestamps.
func syncFileData(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
