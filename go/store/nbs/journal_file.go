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
func (jf *journalFile) writeAt(off int64, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := jf.prepare(off, off+int64(len(p))); err != nil {
		return 0, err
	}
	return jf.f.WriteAt(p, off)
}

func (jf *journalFile) prepare(off, end int64) error {
	if journalPadBufferSize == 0 || end <= jf.preparedThrough {
		return nil
	}
	target := end + journalPadBufferSize
	zeros := make([]byte, min(int64(1<<20), target-off))
	for o := max(jf.preparedThrough, off); o < target; o += int64(len(zeros)) {
		if _, err := jf.f.WriteAt(zeros[:min(int64(len(zeros)), target-o)], o); err != nil {
			return err
		}
	}
	if err := syncFileData(jf.f); err != nil {
		return err
	}
	jf.preparedThrough = target
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
