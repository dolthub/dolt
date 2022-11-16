// Copyright 2022 Dolthub, Inc.
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

import (
	"fmt"
	"io"
	"os"
)

func newJournalWriter(file *os.File, offset int64, size int) *journalWriter {
	return &journalWriter{
		buf:  make([]byte, 0, size),
		file: file,
		off:  offset,
	}
}

type journalWriter struct {
	buf  []byte
	file *os.File
	off  int64
}

var _ io.ReaderAt = &journalWriter{}
var _ io.WriteCloser = &journalWriter{}

func (j *journalWriter) Offset() int64 {
	return j.off + int64(len(j.buf))
}

func (j *journalWriter) ReadAt(p []byte, off int64) (n int, err error) {
	var bp []byte
	if off < j.off {
		// fill some or all of |p| from |j.file|
		fread := int(j.off - off)
		if len(p) > fread {
			// straddled read
			bp = p[fread:]
			p = p[:fread]
		}
		if n, err = j.file.ReadAt(p, off); err != nil {
			return 0, err
		}
		off = 0
	} else {
		// fill all of |p| from |j.buf|
		bp = p
		off -= j.off
	}
	n += copy(bp, j.buf[off:])
	return
}

func (j *journalWriter) GetBytes(n int) (buf []byte, err error) {
	c, l := cap(j.buf), len(j.buf)
	if n > c {
		err = fmt.Errorf("requested bytes (%d) exceeds capacity (%d)", n, c)
		return
	} else if n > c-l {
		if err = j.Flush(); err != nil {
			return
		}
	}
	l = len(j.buf)
	j.buf = j.buf[:l+n]
	buf = j.buf[l : l+n]
	return
}

func (j *journalWriter) Write(p []byte) (n int, err error) {
	if len(p) > len(j.buf) {
		// write directly to |j.file|
		if err = j.Flush(); err != nil {
			return 0, err
		}
		n, err = j.file.WriteAt(p, j.off)
		j.off += int64(n)
		return
	}
	var buf []byte
	if buf, err = j.GetBytes(len(p)); err != nil {
		return 0, err
	}
	n = copy(buf, p)
	return
}

func (j *journalWriter) Flush() (err error) {
	if _, err = j.file.WriteAt(j.buf, j.off); err != nil {
		return err
	}
	j.off += int64(len(j.buf))
	j.buf = j.buf[:0]
	return
}

func (j *journalWriter) Sync() (err error) {
	if err = j.Flush(); err != nil {
		return err
	}
	return j.file.Sync()
}

func (j *journalWriter) Close() (err error) {
	if err = j.Flush(); err != nil {
		return err
	}
	return j.file.Close()
}
