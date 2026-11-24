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

package nbs

import "os"

// journalPadZeros stays in BSS and is shared by all journal padding writes.
var journalPadZeros [journalPadBufferSize]byte

// journalFile writes the chunk journal, zero-padding ahead of writes so that
// syncs do not change the file size.
type journalFile struct {
	f               *os.File
	preparedThrough int64
}

func newJournalFile(f *os.File) *journalFile {
	return &journalFile{f: f}
}

// writeAt pads ahead of |p| when needed. An empty write never pads, leaving
// any bytes past the last record for fsck.
func (jf *journalFile) writeAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, err := jf.f.WriteAt(p, off)
	if err != nil {
		return n, err
	}
	return n, jf.prepare(off + int64(n))
}

// prepare zero-fills past |end|. The next syncData makes it durable.
func (jf *journalFile) prepare(end int64) error {
	if journalPadBufferSize == 0 || end <= jf.preparedThrough {
		return nil
	}
	// The zeros must be written: extending with Truncate leaves a sparse hole,
	// and writing into a hole changes block allocation, which fdatasync must
	// then commit.
	if _, err := jf.f.WriteAt(journalPadZeros[:], end); err != nil {
		return err
	}
	jf.preparedThrough = end + journalPadBufferSize
	return nil
}

func (jf *journalFile) syncData() error {
	return syncFileData(jf.f)
}

// finish truncates padding past |off| and syncs.
func (jf *journalFile) finish(off int64) error {
	if jf.preparedThrough > off {
		if err := jf.f.Truncate(off); err != nil {
			return err
		}
	}
	return jf.f.Sync()
}
