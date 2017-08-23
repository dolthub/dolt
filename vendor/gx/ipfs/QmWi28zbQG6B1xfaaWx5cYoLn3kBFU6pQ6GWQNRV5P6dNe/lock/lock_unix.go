// +build linux darwin freebsd openbsd netbsd dragonfly
// +build !appengine

/*
Copyright 2013 The Go Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lock

import (
	"fmt"
	"io"
	"os"

	"gx/ipfs/QmXPKMT5cT8ajqamSD1YaeEpfeaHvs9AU4MQzte4Bkr6V4/sys/unix"
)

func init() {
	lockFn = lockFcntl
}

func lockFcntl(name string) (io.Closer, error) {
	fi, err := os.Stat(name)
	if err == nil && fi.Size() > 0 {
		return nil, fmt.Errorf("can't Lock file %q: has non-zero size", name)
	}

	f, err := os.Create(name)
	if err != nil {
		return nil, fmt.Errorf("Lock Create of %s failed: %v", name, err)
	}

	err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: int16(os.SEEK_SET),
		Start:  0,
		Len:    0, // 0 means to lock the entire file.
		Pid:    0, // only used by F_GETLK
	})

	if err != nil {
		f.Close()
		return nil, fmt.Errorf("Lock FcntlFlock of %s failed: %v", name, err)
	}
	return &unlocker{f: f, abs: name}, nil
}
