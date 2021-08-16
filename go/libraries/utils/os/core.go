// Copyright 2021 Dolthub, Inc.
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

package os

// This is essentially a drop-in replacement for Go's `os` package, however it has been reworked to handle a flexible
// file system. As many functions do not relate in any way to the file system, they have been copied (with comments)
// directly from the Go source code.

import (
	"errors"
	"io"
	"os"
	"time"

	"github.com/spf13/afero"
)

// The defined file mode bits are the most significant bits of the FileMode.
// The nine least-significant bits are the standard Unix rwxrwxrwx permissions.
// The values of these bits should be considered part of the public API and
// may be used in wire protocols or disk representations: they must not be
// changed, although new bits might be added.
const (
	// The single letters are the abbreviations
	// used by the String method's formatting.
	ModeDir        = os.ModeDir        // d: is a directory
	ModeAppend     = os.ModeAppend     // a: append-only
	ModeExclusive  = os.ModeExclusive  // l: exclusive use
	ModeTemporary  = os.ModeTemporary  // T: temporary file; Plan 9 only
	ModeSymlink    = os.ModeSymlink    // L: symbolic link
	ModeDevice     = os.ModeDevice     // D: device file
	ModeNamedPipe  = os.ModeNamedPipe  // p: named pipe (FIFO)
	ModeSocket     = os.ModeSocket     // S: Unix domain socket
	ModeSetuid     = os.ModeSetuid     // u: setuid
	ModeSetgid     = os.ModeSetgid     // g: setgid
	ModeCharDevice = os.ModeCharDevice // c: Unix character device, when ModeDevice is set
	ModeSticky     = os.ModeSticky     // t: sticky
	ModeIrregular  = os.ModeIrregular  // ?: non-regular file; nothing else is known about this file

	// Mask for the type bits. For regular files, none will be set.
	ModeType = os.ModeType

	ModePerm = os.ModePerm // Unix permission bits, 0o777

	// Exactly one of O_RDONLY, O_WRONLY, or O_RDWR must be specified.
	O_RDONLY = os.O_RDONLY // open the file read-only.
	O_WRONLY = os.O_WRONLY // open the file write-only.
	O_RDWR   = os.O_RDWR   // open the file read-write.
	// The remaining values may be or'ed in to control behavior.
	O_APPEND = os.O_APPEND // append data to the file when writing.
	O_CREATE = os.O_CREATE // create a new file if none exists.
	O_EXCL   = os.O_EXCL   // used with O_CREATE, file must not exist.
	O_SYNC   = os.O_SYNC   // open for synchronous I/O.
	O_TRUNC  = os.O_TRUNC  // truncate regular writable file when opened.
)

type (
	// A Signal represents an operating system signal.
	// The usual underlying implementation is operating system-dependent:
	// on Unix it is syscall.Signal.
	Signal = os.Signal
	// File represents an open file descriptor.
	File = afero.File
	// A FileInfo describes a file and is returned by Stat and Lstat.
	FileInfo = os.FileInfo
	// A FileMode represents a file's mode and permission bits.
	// The bits have the same definition on all systems, so that
	// information about files can be moved from one system
	// to another portably. Not all bits apply to all systems.
	// The only required bit is ModeDir for directories.
	FileMode = os.FileMode
	// PathError records an error and the operation and file path that caused it.
	PathError = os.PathError
	// LinkError records an error during a link or symlink or rename
	// system call and the paths that caused it.
	LinkError = os.LinkError
	// SyscallError records an error from a specific system call.
	SyscallError = os.SyscallError
)

// The only signal values guaranteed to be present in the os package on all
// systems are os.Interrupt (send the process an interrupt) and os.Kill (force
// the process to exit). On Windows, sending os.Interrupt to a process with
// os.Process.Signal is not implemented; it will return an error instead of
// sending a signal.
var (
	Kill              = os.Kill
	Interrupt         = os.Interrupt
	PathSeparator     = os.PathSeparator
	PathListSeparator = os.PathListSeparator
)

// Stdin, Stdout, and Stderr are open Files pointing to the standard input,
// standard output, and standard error file descriptors.
//
// Note that the Go runtime writes to standard error for panics and crashes;
// closing Stderr may cause those messages to go elsewhere, perhaps
// to a file opened later.
var (
	Stdin  afero.File = os.Stdin
	Stdout afero.File = os.Stdout
	Stderr afero.File = os.Stderr
)

var (
	// Args hold the command-line arguments, starting with the program name.
	Args = os.Args

	// ErrInvalid indicates an invalid argument.
	// Methods on File will return this error when the receiver is nil.
	ErrInvalid          = os.ErrInvalid          // "invalid argument"
	ErrPermission       = os.ErrPermission       // "permission denied"
	ErrExist            = os.ErrExist            // "file already exists"
	ErrNotExist         = os.ErrNotExist         // "file does not exist"
	ErrClosed           = os.ErrClosed           // "file already closed"
	ErrNoDeadline       = os.ErrNoDeadline       // "file type does not support deadline"
	ErrDeadlineExceeded = os.ErrDeadlineExceeded // "i/o timeout"

	fileSystem = afero.NewOsFs()
)

// Create creates or truncates the named file. If the file already exists,
// it is truncated. If the file does not exist, it is created with mode 0666
// (before umask). If successful, methods on the returned File can
// be used for I/O; the associated file descriptor has mode O_RDWR.
// If there is an error, it will be of type *PathError.
func Create(name string) (afero.File, error) {
	return fileSystem.Create(name)
}

// Mkdir creates a new directory with the specified name and permission
// bits (before umask).
// If there is an error, it will be of type *PathError.
func Mkdir(name string, perm os.FileMode) error {
	return fileSystem.Mkdir(name, perm)
}

// MkdirAll creates a directory named path,
// along with any necessary parents, and returns nil,
// or else returns an error.
// The permission bits perm (before umask) are used for all
// directories that MkdirAll creates.
// If path is already a directory, MkdirAll does nothing
// and returns nil.
func MkdirAll(path string, perm os.FileMode) error {
	return fileSystem.MkdirAll(path, perm)
}

// Open opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func Open(name string) (afero.File, error) {
	return fileSystem.Open(name)
}

// OpenFile is the generalized open call; most users will use Open
// or Create instead. It opens the named file with specified flag
// (O_RDONLY etc.). If the file does not exist, and the O_CREATE flag
// is passed, it is created with mode perm (before umask). If successful,
// methods on the returned File can be used for I/O.
// If there is an error, it will be of type *PathError.
func OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return fileSystem.OpenFile(name, flag, perm)
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *PathError.
func Stat(name string) (os.FileInfo, error) {
	return fileSystem.Stat(name)
}

// Chmod changes the mode of the named file to mode.
// If the file is a symbolic link, it changes the mode of the link's target.
// If there is an error, it will be of type *PathError.
//
// A different subset of the mode bits are used, depending on the
// operating system.
//
// On Unix, the mode's permission bits, ModeSetuid, ModeSetgid, and
// ModeSticky are used.
//
// On Windows, only the 0200 bit (owner writable) of mode is used; it
// controls whether the file's read-only attribute is set or cleared.
// The other bits are currently unused. For compatibility with Go 1.12
// and earlier, use a non-zero mode. Use mode 0400 for a read-only
// file and 0600 for a readable+writable file.
//
// On Plan 9, the mode's permission bits, ModeAppend, ModeExclusive,
// and ModeTemporary are used.
func Chmod(name string, mode os.FileMode) error {
	return fileSystem.Chmod(name, mode)
}

// Chtimes changes the access and modification times of the named
// file, similar to the Unix utime() or utimes() functions.
//
// The underlying filesystem may truncate or round the values to a
// less precise time unit.
// If there is an error, it will be of type *PathError.
func Chtimes(name string, atime time.Time, mtime time.Time) error {
	return fileSystem.Chtimes(name, atime, mtime)
}

// Chdir changes the current working directory to the named directory.
// If there is an error, it will be of type *PathError.
func Chdir(dir string) error {
	if _, ok := fileSystem.(*afero.OsFs); ok {
		return os.Chdir(dir)
	}
	return errors.New("may only change directory using the operating system's file system")
}

// Getwd returns a rooted path name corresponding to the
// current directory. If the current directory can be
// reached via multiple paths (due to symbolic links),
// Getwd may return any one of them.
func Getwd() (dir string, err error) {
	if _, ok := fileSystem.(*afero.OsFs); ok {
		return os.Getwd()
	}
	return "", errors.New("may only get the working directory using the operating system's file system")
}

// ReadFile reads the named file and returns the contents.
// A successful call returns err == nil, not err == EOF.
// Because ReadFile reads the whole file, it does not treat an EOF from Read
// as an error to be reported.
func ReadFile(name string) ([]byte, error) {
	f, err := Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var size int
	if info, err := f.Stat(); err == nil {
		size64 := info.Size()
		if int64(int(size64)) == size64 {
			size = int(size64)
		}
	}
	size++ // one byte for final read at EOF

	// If a file claims a small size, read at least 512 bytes.
	// In particular, files in Linux's /proc claim size 0 but
	// then do not work right if read in small pieces,
	// so an initial read of 1 byte would not work correctly.
	if size < 512 {
		size = 512
	}

	data := make([]byte, 0, size)
	for {
		if len(data) >= cap(data) {
			d := append(data[:cap(data)], 0)
			data = d[:len(data)]
		}
		n, err := f.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}
	}
}

// WriteFile writes data to the named file, creating it if necessary.
// If the file does not exist, WriteFile creates it with permissions perm (before umask);
// otherwise WriteFile truncates it before writing, without changing permissions.
func WriteFile(name string, data []byte, perm FileMode) error {
	f, err := OpenFile(name, O_WRONLY|O_CREATE|O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

// Environ returns a copy of strings representing the environment,
// in the form "key=value".
func Environ() []string {
	return os.Environ()
}

// Getenv retrieves the value of the environment variable named by the key.
// It returns the value, which will be empty if the variable is not present.
// To distinguish between an empty value and an unset value, use LookupEnv.
func Getenv(key string) string {
	return os.Getenv(key)
}

// IsExist returns a boolean indicating whether the error is known to report
// that a file or directory already exists. It is satisfied by ErrExist as
// well as some syscall errors.
//
// This function predates errors.Is. It only supports errors returned by
// the os package. New code should use errors.Is(err, os.ErrExist).
func IsExist(err error) bool {
	return os.IsExist(err)
}

// Getpid returns the process id of the caller.
func Getpid() int {
	return os.Getpid()
}

// Geteuid returns the numeric effective user id of the caller.
//
// On Windows, it returns -1.
func Geteuid() int {
	return os.Geteuid()
}

// LookupEnv retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned and the boolean is true.
// Otherwise the returned value will be empty and the boolean will
// be false.
func LookupEnv(key string) (string, bool) {
	return os.LookupEnv(key)
}

// Getpagesize returns the underlying system's memory page size.
func Getpagesize() int {
	return os.Getpagesize()
}

// Exit causes the current program to exit with the given status code.
// Conventionally, code zero indicates success, non-zero an error.
// The program terminates immediately; deferred functions are not run.
//
// For portability, the status code should be in the range [0, 125].
func Exit(code int) {
	os.Exit(code)
}

// TempDir returns the default directory to use for temporary files.
//
// On Unix systems, it returns $TMPDIR if non-empty, else /tmp.
// On Windows, it uses GetTempPath, returning the first non-empty
// value from %TMP%, %TEMP%, %USERPROFILE%, or the Windows directory.
// On Plan 9, it returns /tmp.
//
// The directory is neither guaranteed to exist nor have accessible
// permissions.
func TempDir() string {
	if _, ok := fileSystem.(*afero.OsFs); ok {
		return os.TempDir()
	}
	return afero.GetTempDir(fileSystem, "")
}

// IsNotExist returns a boolean indicating whether the error is known to
// report that a file or directory does not exist. It is satisfied by
// ErrNotExist as well as some syscall errors.
//
// This function predates errors.Is. It only supports errors returned by
// the os package. New code should use errors.Is(err, os.ErrNotExist).
func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// GetFileSystem returns the file system that is currently in use.
func GetFileSystem() afero.Fs {
	return fileSystem
}

// Pipe returns a connected pair of Files; reads from r return bytes written to w.
// It returns the files and an error, if any.
func Pipe() (r File, w File, err error) {
	if _, ok := fileSystem.(*afero.OsFs); ok {
		return os.Pipe()
	}
	return nil, nil, errors.New("may only create pipes using the operating system's file system")
}

// UserHomeDir returns the current user's home directory.
//
// On Unix, including macOS, it returns the $HOME environment variable.
// On Windows, it returns %USERPROFILE%.
// On Plan 9, it returns the $home environment variable.
func UserHomeDir() (string, error) {
	return os.UserHomeDir()
}

// SetFileSystem updates the used file system to the one given. This is primarily used to switch to an in-memory
// representation for testing. By default, we use the operating system's file system.
func SetFileSystem(fs afero.Fs) {
	fileSystem = fs
}
