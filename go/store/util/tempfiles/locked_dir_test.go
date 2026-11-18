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

package tempfiles

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	filelocks "github.com/dolthub/file-locks"
	"github.com/stretchr/testify/require"
)

// The locks belong to the open file rather than the process, so another Lock in this process
// stands in for another process.
func otherProcess(t *testing.T, dir string) *filelocks.Lock {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o755))
	lock, err := filelocks.New(dir + lockedDirLockSuffix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	return lock
}

func newTestLockedDir(t *testing.T, parent string) *LockedDir {
	t.Helper()
	d, err := NewLockedDir(parent)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.lock.Close() })
	return d
}

// writeLockedDir makes a LockedDir in |parent| holding a file, with nobody holding its lock.
func writeLockedDir(t *testing.T, parent, name string) (string, string) {
	t.Helper()
	dir := filepath.Join(parent, lockedDirPrefix+name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(dir+lockedDirLockSuffix, nil, 0o644))
	path := filepath.Join(dir, "file-"+name)
	require.NoError(t, os.WriteFile(path, []byte("contents"), 0o644))
	return dir, path
}

func TestNewLockedDir(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "not", "created", "yet")
	d := newTestLockedDir(t, parent)
	require.Equal(t, parent, filepath.Dir(d.dir))
	require.Equal(t, filelocks.ErrLocked, otherProcess(t, d.dir).TryLock())

	f1, err := d.NewFile("file-")
	require.NoError(t, err)
	f2, err := d.NewFile("file-")
	require.NoError(t, err)
	require.Equal(t, d.dir, filepath.Dir(f1.Name()))
	require.Equal(t, d.dir, filepath.Dir(f2.Name()))

	require.NoError(t, d.RemoveFile(f1))
	_, err = os.Stat(f1.Name())
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.RemoveFile(f2))
}

func TestNewLockFile(t *testing.T) {
	parent := t.TempDir()
	p1, err := newLockFile(parent)
	require.NoError(t, err)
	p2, err := newLockFile(parent)
	require.NoError(t, err)
	require.NotEqual(t, p1, p2)
	for _, p := range []string{p1, p2} {
		require.Equal(t, parent, filepath.Dir(p))
		require.True(t, strings.HasPrefix(filepath.Base(p), lockedDirPrefix), p)
		require.True(t, strings.HasSuffix(p, lockedDirLockSuffix), p)
		info, err := os.Stat(p)
		require.NoError(t, err)
		require.Zero(t, info.Size())
	}

	require.ErrorIs(t, createLockFile(p1), os.ErrExist, "a lock file is only ever made new")
	_, err = newLockFile(filepath.Join(parent, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestNewLockedDir_DoesNotTouchOtherDirs(t *testing.T) {
	parent := t.TempDir()
	_, path := writeLockedDir(t, parent, "other")
	newTestLockedDir(t, parent)
	_, err := os.Stat(path)
	require.NoError(t, err)
}

func TestNewLockedDir_ReturnsErrors(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a directory this user cannot write to")
	}
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o555))
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	_, err := NewLockedDir(parent)
	require.ErrorIs(t, err, os.ErrPermission)
}

func TestNewFile_FailsLoudlyIfDirIsRemoved(t *testing.T) {
	d := newTestLockedDir(t, t.TempDir())
	require.NoError(t, os.RemoveAll(d.dir))
	_, err := d.NewFile("file-")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestMayBeDeletePending(t *testing.T) {
	require.False(t, mayBeDeletePending(nil))
	require.False(t, mayBeDeletePending(errors.New("boom")))
	require.False(t, mayBeDeletePending(filelocks.ErrLocked))
	require.False(t, mayBeDeletePending(&fs.PathError{Op: "open", Path: "x.lock", Err: fs.ErrNotExist}))
	// Windows reports a file which is being deleted as access denied.
	require.Equal(t, runtime.GOOS == "windows", mayBeDeletePending(&fs.PathError{Op: "open", Path: "x.lock", Err: fs.ErrPermission}))

	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		return
	}
	// The error from file-locks has to unwrap to its cause.
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o555))
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	_, err := tryLockFile(filepath.Join(parent, "x.lock"))
	require.ErrorIs(t, err, fs.ErrPermission)
}

func TestRetryWhile(t *testing.T) {
	pending := &fs.PathError{Op: "open", Path: "x.lock", Err: fs.ErrPermission}
	isPending := func(err error) bool { return errors.Is(err, fs.ErrPermission) }
	lock, err := filelocks.New(filepath.Join(t.TempDir(), "x.lock"))
	require.NoError(t, err)
	defer lock.Close()

	t.Run("a deletion which finishes", func(t *testing.T) {
		calls := 0
		got, err := retryWhile(time.Second, isPending, func() (*filelocks.Lock, error) {
			if calls++; calls < 3 {
				return nil, pending
			}
			return lock, nil
		})
		require.NoError(t, err)
		require.Same(t, lock, got)
		require.Equal(t, 3, calls)
	})
	t.Run("a real permission problem", func(t *testing.T) {
		calls := 0
		start := time.Now()
		_, err := retryWhile(20*time.Millisecond, isPending, func() (*filelocks.Lock, error) {
			calls++
			return nil, pending
		})
		require.ErrorIs(t, err, fs.ErrPermission, "it is returned once the wait is over")
		require.Greater(t, calls, 1)
		require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	})
	t.Run("other errors", func(t *testing.T) {
		calls := 0
		_, err := retryWhile(time.Second, isPending, func() (*filelocks.Lock, error) {
			calls++
			return nil, filelocks.ErrLocked
		})
		require.Equal(t, filelocks.ErrLocked, err)
		require.Equal(t, 1, calls, "only an error which may be a deletion is retried")
	})
}

func TestUnlockDir_PanicsOnError(t *testing.T) {
	dir := t.TempDir()
	lock, err := filelocks.New(filepath.Join(dir, "LOCK"))
	require.NoError(t, err)
	defer lock.Close()
	// Unlocking a lock which is not held is an error.
	require.Panics(t, func() { unlockDir(lock, dir) })
}

func TestCleanDir(t *testing.T) {
	t.Run("missing directory", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "spools")
		require.NoError(t, CleanDir(parent))
		_, err := os.Stat(parent)
		require.True(t, os.IsNotExist(err), "cleaning must not make the directory")
	})
	t.Run("dead process", func(t *testing.T) {
		parent := t.TempDir()
		dir, _ := writeLockedDir(t, parent, "dead")
		require.NoError(t, CleanDir(parent))
		_, err := os.Stat(dir)
		require.True(t, os.IsNotExist(err), "the dead process's directory must be removed")
		_, err = os.Stat(dir + lockedDirLockSuffix)
		require.True(t, os.IsNotExist(err), "the dead process's lock file must be removed")
	})
	t.Run("live process", func(t *testing.T) {
		parent := t.TempDir()
		dir, path := writeLockedDir(t, parent, "live")
		require.NoError(t, otherProcess(t, dir).TryLock())
		require.NoError(t, CleanDir(parent), "a live process is not an error")
		_, err := os.Stat(path)
		require.NoError(t, err, "a live process may be using this file")
	})
	t.Run("dead process next to a live one", func(t *testing.T) {
		parent := t.TempDir()
		liveDir, live := writeLockedDir(t, parent, "live")
		require.NoError(t, otherProcess(t, liveDir).TryLock())
		deadDir, _ := writeLockedDir(t, parent, "dead")
		require.NoError(t, CleanDir(parent))
		_, err := os.Stat(live)
		require.NoError(t, err)
		_, err = os.Stat(deadDir)
		require.True(t, os.IsNotExist(err), "a live process must not keep a dead one's files around")
	})
	t.Run("this process is using it", func(t *testing.T) {
		parent := t.TempDir()
		d := newTestLockedDir(t, parent)
		f, err := d.NewFile("file-")
		require.NoError(t, err)
		defer d.RemoveFile(f)
		require.NoError(t, CleanDir(parent))
		_, err = os.Stat(f.Name())
		require.NoError(t, err)
	})
	t.Run("lock file cannot be opened", func(t *testing.T) {
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("needs a file this user cannot open")
		}
		parent := t.TempDir()
		dir, path := writeLockedDir(t, parent, "unreadable")
		require.NoError(t, os.Chmod(dir+lockedDirLockSuffix, 0o000))
		t.Cleanup(func() { _ = os.Chmod(dir+lockedDirLockSuffix, 0o644) })
		require.ErrorIs(t, CleanDir(parent), os.ErrPermission, "a real permission problem must be reported")
		_, err := os.Stat(path)
		require.NoError(t, err)
	})
	t.Run("cleanup fails", func(t *testing.T) {
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("needs a directory this user cannot remove files from")
		}
		parent := t.TempDir()
		dir, _ := writeLockedDir(t, parent, "stuck")
		lock := otherProcess(t, dir)
		require.NoError(t, os.Chmod(dir, 0o555))
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
		require.ErrorIs(t, CleanDir(parent), os.ErrPermission)
		require.NoError(t, lock.TryLock(), "a failed cleanup must not keep the lock")
	})
}

// Several cleanups racing each other and a process making its directory must neither fail nor
// take away a directory whose lock is held.
func TestNewLockedDir_RacingClean(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "spools")

	const cleaners = 4
	stop := make(chan struct{})
	cleaned := make(chan error, cleaners)
	for i := 0; i < cleaners; i++ {
		go func() {
			for {
				select {
				case <-stop:
					cleaned <- nil
					return
				default:
				}
				if err := CleanDir(parent); err != nil {
					cleaned <- err
					return
				}
			}
		}()
	}
	defer func() {
		close(stop)
		for i := 0; i < cleaners; i++ {
			require.NoError(t, <-cleaned)
		}
	}()

	for i := 0; i < 2000; i++ {
		d, err := NewLockedDir(parent)
		require.NoError(t, err, "iteration %d", i)
		_, err = os.Stat(d.dir)
		require.NoError(t, err, "iteration %d: a directory whose lock is held must not be removed", i)
		_, err = os.Stat(d.dir + lockedDirLockSuffix)
		require.NoError(t, err, "iteration %d: a held lock file must not be removed", i)
		require.NoError(t, d.lock.Close())
	}
}

func TestLockedDir_Close(t *testing.T) {
	parent := t.TempDir()
	stat := func(path string) error {
		_, err := os.Stat(path)
		return err
	}

	t.Run("with no files", func(t *testing.T) {
		d := newTestLockedDir(t, parent)
		require.NoError(t, d.Close())
		require.True(t, os.IsNotExist(stat(d.dir)))
		require.True(t, os.IsNotExist(stat(d.dir+lockedDirLockSuffix)))
		require.NoError(t, d.Close(), "closing twice is harmless")
		_, err := d.NewFile("file-")
		require.ErrorIs(t, err, ErrLockedDirClosed)
	})
	t.Run("files keep it until the last is removed", func(t *testing.T) {
		d := newTestLockedDir(t, parent)
		f1, err := d.NewFile("file-")
		require.NoError(t, err)
		f2, err := d.NewFile("file-")
		require.NoError(t, err)
		other := otherProcess(t, d.dir)

		require.NoError(t, d.Close())
		require.NoError(t, stat(f1.Name()), "a file may still be in use")
		require.Equal(t, filelocks.ErrLocked, other.TryLock())
		_, err = d.NewFile("file-")
		require.ErrorIs(t, err, ErrLockedDirClosed)

		require.NoError(t, d.RemoveFile(f1))
		require.NoError(t, stat(d.dir))
		require.Equal(t, filelocks.ErrLocked, other.TryLock())

		require.NoError(t, d.RemoveFile(f2))
		require.True(t, os.IsNotExist(stat(d.dir)))
		require.True(t, os.IsNotExist(stat(d.dir+lockedDirLockSuffix)))
	})
	t.Run("removing a file while open keeps it", func(t *testing.T) {
		d := newTestLockedDir(t, parent)
		f, err := d.NewFile("file-")
		require.NoError(t, err)
		require.NoError(t, d.RemoveFile(f))
		require.NoError(t, stat(d.dir), "only Close gives the directory up")
		require.Equal(t, filelocks.ErrLocked, otherProcess(t, d.dir).TryLock())
		require.NoError(t, d.Close())
	})
}

// blockingTempFileProvider holds NewFile until the test lets it go on.
type blockingTempFileProvider struct {
	TempFileProvider
	entered chan struct{}
	proceed chan struct{}
}

func (p blockingTempFileProvider) NewFile(dir, pattern string) (*os.File, error) {
	close(p.entered)
	<-p.proceed
	return p.TempFileProvider.NewFile(dir, pattern)
}

// A Close that runs while a file is being made has to wait for it, and then count it. If it did
// not wait, it would remove the directory the file is about to go in.
func TestLockedDir_CloseWaitsForFileBeingMade(t *testing.T) {
	d := newTestLockedDir(t, t.TempDir())

	provider := blockingTempFileProvider{MovableTempFileProvider, make(chan struct{}), make(chan struct{})}
	MovableTempFileProvider = provider
	t.Cleanup(func() { MovableTempFileProvider = provider.TempFileProvider })

	type result struct {
		f   *os.File
		err error
	}
	made := make(chan result, 1)
	go func() {
		f, err := d.NewFile("file-")
		made <- result{f, err}
	}()
	<-provider.entered

	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case <-closed:
		close(provider.proceed)
		<-made
		require.Fail(t, "Close ran while a file was being made")
	case <-time.After(100 * time.Millisecond):
	}

	close(provider.proceed)
	res := <-made
	require.NoError(t, res.err)
	require.NoError(t, <-closed)
	require.Equal(t, filelocks.ErrLocked, otherProcess(t, d.dir).TryLock(), "Close must count the file and keep the lock")
	require.NoError(t, d.RemoveFile(res.f))
	_, err := os.Stat(d.dir)
	require.True(t, os.IsNotExist(err))
}
