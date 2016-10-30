// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type knownFs struct {
	unionFS pathfs.FileSystem
	nodeFS  *pathfs.PathNodeFs
}

// Creates unions for all files under a given directory,
// walking the tree and looking for directories D which have a
// D/READONLY symlink.
//
// A union for A/B/C will placed under directory A-B-C.
type autoUnionFs struct {
	pathfs.FileSystem
	debug bool

	lock             sync.RWMutex
	zombies          map[string]bool
	knownFileSystems map[string]knownFs
	nameRootMap      map[string]string
	root             string

	nodeFs  *pathfs.PathNodeFs
	options *AutoUnionFsOptions
}

type AutoUnionFsOptions struct {
	UnionFsOptions

	nodefs.Options
	pathfs.PathNodeFsOptions

	// If set, run updateKnownFses() after mounting.
	UpdateOnMount bool

	// If set hides the _READONLY file.
	HideReadonly bool

	// Expose this version in /status/gounionfs_version
	Version string
}

const (
	_READONLY    = "READONLY"
	_STATUS      = "status"
	_CONFIG      = "config"
	_DEBUG       = "debug"
	_ROOT        = "root"
	_VERSION     = "gounionfs_version"
	_SCAN_CONFIG = ".scan_config"
)

func NewAutoUnionFs(directory string, options AutoUnionFsOptions) pathfs.FileSystem {
	if options.HideReadonly {
		options.HiddenFiles = append(options.HiddenFiles, _READONLY)
	}
	a := &autoUnionFs{
		knownFileSystems: make(map[string]knownFs),
		nameRootMap:      make(map[string]string),
		zombies:          make(map[string]bool),
		options:          &options,
		FileSystem:       pathfs.NewDefaultFileSystem(),
	}

	directory, err := filepath.Abs(directory)
	if err != nil {
		panic("filepath.Abs returned err")
	}
	a.root = directory
	return a
}

func (fs *autoUnionFs) String() string {
	return fmt.Sprintf("autoUnionFs(%s)", fs.root)
}

func (fs *autoUnionFs) OnMount(nodeFs *pathfs.PathNodeFs) {
	fs.nodeFs = nodeFs
	if fs.options.UpdateOnMount {
		time.AfterFunc(100*time.Millisecond, func() { fs.updateKnownFses() })
	}
}

func (fs *autoUnionFs) addAutomaticFs(roots []string) {
	relative := strings.TrimLeft(strings.Replace(roots[0], fs.root, "", -1), "/")
	name := strings.Replace(relative, "/", "-", -1)

	if fs.getUnionFs(name) == nil {
		fs.addFs(name, roots)
	}
}

func (fs *autoUnionFs) createFs(name string, roots []string) fuse.Status {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if fs.zombies[name] {
		log.Printf("filesystem named %q is being removed", name)
		return fuse.EBUSY
	}

	for workspace, root := range fs.nameRootMap {
		if root == roots[0] && workspace != name {
			log.Printf("Already have a union FS for directory %s in workspace %s",
				roots[0], workspace)
			return fuse.EBUSY
		}
	}

	known := fs.knownFileSystems[name]
	if known.unionFS != nil {
		log.Println("Already have a workspace:", name)
		return fuse.EBUSY
	}

	ufs, err := NewUnionFsFromRoots(roots, &fs.options.UnionFsOptions, true)
	if err != nil {
		log.Println("Could not create UnionFs:", err)
		return fuse.EPERM
	}

	log.Printf("Adding workspace %v for roots %v", name, ufs.String())
	nfs := pathfs.NewPathNodeFs(ufs, &fs.options.PathNodeFsOptions)
	code := fs.nodeFs.Mount(name, nfs.Root(), &fs.options.Options)
	if code.Ok() {
		fs.knownFileSystems[name] = knownFs{
			ufs,
			nfs,
		}
		fs.nameRootMap[name] = roots[0]
	}
	return code
}

func (fs *autoUnionFs) rmFs(name string) (code fuse.Status) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if fs.zombies[name] {
		return fuse.ENOENT
	}

	known := fs.knownFileSystems[name]
	if known.unionFS == nil {
		return fuse.ENOENT
	}

	root := fs.nameRootMap[name]
	delete(fs.knownFileSystems, name)
	delete(fs.nameRootMap, name)
	fs.zombies[name] = true
	fs.lock.Unlock()
	code = fs.nodeFs.Unmount(name)

	fs.lock.Lock()
	delete(fs.zombies, name)
	if !code.Ok() {
		// Reinstate.
		log.Printf("Unmount failed for %s.  Code %v", name, code)
		fs.knownFileSystems[name] = known
		fs.nameRootMap[name] = root
	}
	return code
}

func (fs *autoUnionFs) addFs(name string, roots []string) (code fuse.Status) {
	if name == _CONFIG || name == _STATUS || name == _SCAN_CONFIG {
		return fuse.EINVAL
	}
	return fs.createFs(name, roots)
}

func (fs *autoUnionFs) getRoots(path string) []string {
	ro := filepath.Join(path, _READONLY)
	fi, err := os.Lstat(ro)
	fiDir, errDir := os.Stat(ro)
	if err != nil || errDir != nil {
		return nil
	}

	if fi.Mode()&os.ModeSymlink != 0 && fiDir.IsDir() {
		// TODO - should recurse and chain all READONLYs
		// together.
		return []string{path, ro}
	}
	return nil
}

func (fs *autoUnionFs) visit(path string, fi os.FileInfo, err error) error {
	if fi != nil && fi.IsDir() {
		roots := fs.getRoots(path)
		if roots != nil {
			fs.addAutomaticFs(roots)
		}
	}
	return nil
}

func (fs *autoUnionFs) updateKnownFses() {
	// We unroll the first level of entries in the root manually in order
	// to allow symbolic links on that level.
	directoryEntries, err := ioutil.ReadDir(fs.root)
	if err == nil {
		for _, dir := range directoryEntries {
			if dir.IsDir() || dir.Mode()&os.ModeSymlink != 0 {
				path := filepath.Join(fs.root, dir.Name())
				dir, _ = os.Stat(path)
				fs.visit(path, dir, nil)
				filepath.Walk(path,
					func(path string, fi os.FileInfo, err error) error {
						return fs.visit(path, fi, err)
					})
			}
		}
	}
}

func (fs *autoUnionFs) Readlink(path string, context *fuse.Context) (out string, code fuse.Status) {
	comps := strings.Split(path, string(filepath.Separator))
	if comps[0] == _STATUS && comps[1] == _ROOT {
		return fs.root, fuse.OK
	}

	if comps[0] != _CONFIG {
		return "", fuse.ENOENT
	}

	name := comps[1]

	fs.lock.RLock()
	defer fs.lock.RUnlock()

	root, ok := fs.nameRootMap[name]
	if ok {
		return root, fuse.OK
	}
	return "", fuse.ENOENT
}

func (fs *autoUnionFs) getUnionFs(name string) pathfs.FileSystem {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	return fs.knownFileSystems[name].unionFS
}

func (fs *autoUnionFs) Symlink(pointedTo string, linkName string, context *fuse.Context) (code fuse.Status) {
	comps := strings.Split(linkName, "/")
	if len(comps) != 2 {
		return fuse.EPERM
	}

	if comps[0] == _CONFIG {
		roots := fs.getRoots(pointedTo)
		if roots == nil {
			return fuse.Status(syscall.ENOTDIR)
		}

		name := comps[1]
		return fs.addFs(name, roots)
	}
	return fuse.EPERM
}

func (fs *autoUnionFs) Unlink(path string, context *fuse.Context) (code fuse.Status) {
	comps := strings.Split(path, "/")
	if len(comps) != 2 {
		return fuse.EPERM
	}

	if comps[0] == _CONFIG && comps[1] != _SCAN_CONFIG {
		code = fs.rmFs(comps[1])
	} else {
		code = fuse.ENOENT
	}
	return code
}

// Must define this, because ENOSYS will suspend all GetXAttr calls.
func (fs *autoUnionFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	return nil, fuse.ENOATTR
}

func (fs *autoUnionFs) GetAttr(path string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	a := &fuse.Attr{
		Owner: *fuse.CurrentOwner(),
	}
	if path == "" || path == _CONFIG || path == _STATUS {
		a.Mode = fuse.S_IFDIR | 0755
		return a, fuse.OK
	}

	if path == filepath.Join(_STATUS, _VERSION) {
		a.Mode = fuse.S_IFREG | 0644
		a.Size = uint64(len(fs.options.Version))
		return a, fuse.OK
	}

	if path == filepath.Join(_STATUS, _DEBUG) {
		a.Mode = fuse.S_IFREG | 0644
		a.Size = uint64(len(fs.DebugData()))
		return a, fuse.OK
	}

	if path == filepath.Join(_STATUS, _ROOT) {
		a.Mode = syscall.S_IFLNK | 0644
		return a, fuse.OK
	}

	if path == filepath.Join(_CONFIG, _SCAN_CONFIG) {
		a.Mode = fuse.S_IFREG | 0644
		return a, fuse.OK
	}
	comps := strings.Split(path, string(filepath.Separator))

	if len(comps) > 1 && comps[0] == _CONFIG {
		fs := fs.getUnionFs(comps[1])

		if fs == nil {
			return nil, fuse.ENOENT
		}

		a.Mode = syscall.S_IFLNK | 0644
		return a, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (fs *autoUnionFs) StatusDir() (stream []fuse.DirEntry, status fuse.Status) {
	stream = make([]fuse.DirEntry, 0, 10)
	stream = []fuse.DirEntry{
		{Name: _VERSION, Mode: fuse.S_IFREG | 0644},
		{Name: _DEBUG, Mode: fuse.S_IFREG | 0644},
		{Name: _ROOT, Mode: syscall.S_IFLNK | 0644},
	}
	return stream, fuse.OK
}

func (fs *autoUnionFs) DebugData() string {
	conn := fs.nodeFs.Connector()
	if conn.Server() == nil {
		return "autoUnionFs.mountState not set"
	}
	setting := conn.Server().KernelSettings()
	msg := fmt.Sprintf(
		"Version: %v\n"+
			"Bufferpool: %v\n"+
			"Kernel: %v\n",
		fs.options.Version,
		conn.Server().DebugData(),
		&setting)

	if conn != nil {
		msg += fmt.Sprintf("Live inodes: %d\n", conn.InodeHandleCount())
	}

	return msg
}

func (fs *autoUnionFs) Open(path string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	if path == filepath.Join(_STATUS, _DEBUG) {
		if flags&fuse.O_ANYWRITE != 0 {
			return nil, fuse.EPERM
		}

		return nodefs.NewDataFile([]byte(fs.DebugData())), fuse.OK
	}
	if path == filepath.Join(_STATUS, _VERSION) {
		if flags&fuse.O_ANYWRITE != 0 {
			return nil, fuse.EPERM
		}
		return nodefs.NewDataFile([]byte(fs.options.Version)), fuse.OK
	}
	if path == filepath.Join(_CONFIG, _SCAN_CONFIG) {
		if flags&fuse.O_ANYWRITE != 0 {
			fs.updateKnownFses()
		}
		return nodefs.NewDevNullFile(), fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *autoUnionFs) Truncate(name string, offset uint64, context *fuse.Context) (code fuse.Status) {
	if name != filepath.Join(_CONFIG, _SCAN_CONFIG) {
		log.Println("Huh? Truncating unsupported write file", name)
		return fuse.EPERM
	}
	return fuse.OK
}

func (fs *autoUnionFs) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	switch name {
	case _STATUS:
		return fs.StatusDir()
	case _CONFIG:
	case "/":
		name = ""
	case "":
	default:
		log.Printf("Argh! Don't know how to list dir %v", name)
		return nil, fuse.ENOSYS
	}

	fs.lock.RLock()
	defer fs.lock.RUnlock()

	stream = make([]fuse.DirEntry, 0, len(fs.knownFileSystems)+5)
	if name == _CONFIG {
		for k := range fs.knownFileSystems {
			stream = append(stream, fuse.DirEntry{
				Name: k,
				Mode: syscall.S_IFLNK | 0644,
			})
		}
	}

	if name == "" {
		stream = append(stream, fuse.DirEntry{
			Name: _CONFIG,
			Mode: uint32(fuse.S_IFDIR | 0755),
		},
			fuse.DirEntry{
				Name: _STATUS,
				Mode: uint32(fuse.S_IFDIR | 0755),
			})
	}
	return stream, status
}

func (fs *autoUnionFs) StatFs(name string) *fuse.StatfsOut {
	return &fuse.StatfsOut{}
}
