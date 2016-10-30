// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package unionfs

import (
	"crypto/md5"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func filePathHash(path string) string {
	dir, base := filepath.Split(path)

	h := md5.New()
	h.Write([]byte(dir))
	return fmt.Sprintf("%x-%s", h.Sum(nil)[:8], base)
}

/*

 UnionFs implements a user-space union file system, which is
 stateless but efficient even if the writable branch is on NFS.


 Assumptions:

 * It uses a list of branches, the first of which (index 0) is thought
 to be writable, and the rest read-only.

 * It assumes that the number of deleted files is small relative to
 the total tree size.


 Implementation notes.

 * It overlays arbitrary writable FileSystems with any number of
   readonly FileSystems.

 * Deleting a file will put a file named
 /DELETIONS/HASH-OF-FULL-FILENAME into the writable overlay,
 containing the full filename itself.

 This is optimized for NFS usage: we want to minimize the number of
 NFS operations, which are slow.  By putting all whiteouts in one
 place, we can cheaply fetch the list of all deleted files.  Even
 without caching on our side, the kernel's negative dentry cache can
 answer is-deleted queries quickly.

*/
type unionFS struct {
	pathfs.FileSystem

	// The same, but as interfaces.
	fileSystems []pathfs.FileSystem

	// A file-existence cache.
	deletionCache *dirCache

	// A file -> branch cache.
	branchCache *TimedCache

	// Map of files to hide.
	hiddenFiles map[string]bool

	options *UnionFsOptions
	nodeFs  *pathfs.PathNodeFs
}

type UnionFsOptions struct {
	BranchCacheTTL   time.Duration
	DeletionCacheTTL time.Duration
	DeletionDirName  string
	HiddenFiles      []string
}

const (
	_DROP_CACHE = ".drop_cache"
)

func NewUnionFs(fileSystems []pathfs.FileSystem, options UnionFsOptions) (pathfs.FileSystem, error) {
	g := &unionFS{
		options:     &options,
		fileSystems: fileSystems,
		FileSystem:  pathfs.NewDefaultFileSystem(),
	}

	writable := g.fileSystems[0]
	code := g.createDeletionStore()
	if !code.Ok() {
		return nil, fmt.Errorf("could not create deletion path %v: %v", options.DeletionDirName, code)
	}

	g.deletionCache = newDirCache(writable, options.DeletionDirName, options.DeletionCacheTTL)
	g.branchCache = NewTimedCache(
		func(n string) (interface{}, bool) { return g.getBranchAttrNoCache(n), true },
		options.BranchCacheTTL)

	g.hiddenFiles = make(map[string]bool)
	for _, name := range options.HiddenFiles {
		g.hiddenFiles[name] = true
	}

	return g, nil
}

func (fs *unionFS) OnMount(nodeFs *pathfs.PathNodeFs) {
	fs.nodeFs = nodeFs
}

////////////////
// Deal with all the caches.

// The isDeleted() method tells us if a path has a marker in the deletion store.
// It may return an error code if the store could not be accessed.
func (fs *unionFS) isDeleted(name string) (deleted bool, code fuse.Status) {
	marker := fs.deletionPath(name)
	haveCache, found := fs.deletionCache.HasEntry(filepath.Base(marker))
	if haveCache {
		return found, fuse.OK
	}

	_, code = fs.fileSystems[0].GetAttr(marker, nil)

	if code == fuse.OK {
		return true, code
	}
	if code == fuse.ENOENT {
		return false, fuse.OK
	}

	log.Printf("error accessing deletion marker %s: %v", marker, code)
	return false, fuse.Status(syscall.EROFS)
}

func (fs *unionFS) createDeletionStore() (code fuse.Status) {
	writable := fs.fileSystems[0]
	fi, code := writable.GetAttr(fs.options.DeletionDirName, nil)
	if code == fuse.ENOENT {
		code = writable.Mkdir(fs.options.DeletionDirName, 0755, nil)
		if code.Ok() {
			fi, code = writable.GetAttr(fs.options.DeletionDirName, nil)
		}
	}

	if !code.Ok() || !fi.IsDir() {
		code = fuse.Status(syscall.EROFS)
	}

	return code
}

func (fs *unionFS) getBranch(name string) branchResult {
	name = stripSlash(name)
	r := fs.branchCache.Get(name)
	return r.(branchResult)
}

func (fs *unionFS) setBranch(name string, r branchResult) {
	if !r.valid() {
		log.Panicf("entry %q setting illegal branchResult %v", name, r)
	}
	fs.branchCache.Set(name, r)
}

type branchResult struct {
	attr   *fuse.Attr
	code   fuse.Status
	branch int
}

func (r *branchResult) valid() bool {
	return (r.branch >= 0 && r.attr != nil && r.code.Ok()) ||
		(r.branch < 0 && r.attr == nil && !r.code.Ok())
}

func (fs branchResult) String() string {
	return fmt.Sprintf("{%v %v branch %d}", fs.attr, fs.code, fs.branch)
}

func (fs *unionFS) getBranchAttrNoCache(name string) branchResult {
	name = stripSlash(name)

	parent, base := path.Split(name)
	parent = stripSlash(parent)

	parentBranch := 0
	if base != "" {
		parentBranch = fs.getBranch(parent).branch
	}
	for i, fs := range fs.fileSystems {
		if i < parentBranch {
			continue
		}

		a, s := fs.GetAttr(name, nil)
		if s.Ok() {
			if i > 0 {
				// Needed to make hardlinks work.
				a.Ino = 0
			}
			return branchResult{
				attr:   a,
				code:   s,
				branch: i,
			}
		} else {
			if s != fuse.ENOENT {
				log.Printf("getattr: %v:  Got error %v from branch %v", name, s, i)
			}
		}
	}
	return branchResult{nil, fuse.ENOENT, -1}
}

////////////////
// Deletion.

func (fs *unionFS) deletionPath(name string) string {
	return filepath.Join(fs.options.DeletionDirName, filePathHash(name))
}

func (fs *unionFS) removeDeletion(name string) {
	marker := fs.deletionPath(name)
	fs.deletionCache.RemoveEntry(path.Base(marker))

	// os.Remove tries to be smart and issues a Remove() and
	// Rmdir() sequentially.  We want to skip the 2nd system call,
	// so use syscall.Unlink() directly.

	code := fs.fileSystems[0].Unlink(marker, nil)
	if !code.Ok() && code != fuse.ENOENT {
		log.Printf("error unlinking %s: %v", marker, code)
	}
}

func (fs *unionFS) putDeletion(name string) (code fuse.Status) {
	code = fs.createDeletionStore()
	if !code.Ok() {
		return code
	}

	marker := fs.deletionPath(name)
	fs.deletionCache.AddEntry(path.Base(marker))

	// Is there a WriteStringToFileOrDie ?
	writable := fs.fileSystems[0]
	fi, code := writable.GetAttr(marker, nil)
	if code.Ok() && fi.Size == uint64(len(name)) {
		return fuse.OK
	}

	var f nodefs.File
	if code == fuse.ENOENT {
		f, code = writable.Create(marker, uint32(os.O_TRUNC|os.O_WRONLY), 0644, nil)
	} else {
		writable.Chmod(marker, 0644, nil)
		f, code = writable.Open(marker, uint32(os.O_TRUNC|os.O_WRONLY), nil)
	}
	if !code.Ok() {
		log.Printf("could not create deletion file %v: %v", marker, code)
		return fuse.EPERM
	}
	defer f.Release()
	defer f.Flush()
	n, code := f.Write([]byte(name), 0)
	if int(n) != len(name) || !code.Ok() {
		panic(fmt.Sprintf("Error for writing %v: %v, %v (exp %v) %v", name, marker, n, len(name), code))
	}

	return fuse.OK
}

////////////////
// Promotion.

func (fs *unionFS) Promote(name string, srcResult branchResult, context *fuse.Context) (code fuse.Status) {
	writable := fs.fileSystems[0]
	sourceFs := fs.fileSystems[srcResult.branch]

	// Promote directories.
	fs.promoteDirsTo(name)

	if srcResult.attr.IsRegular() {
		code = pathfs.CopyFile(sourceFs, writable, name, name, context)

		if code.Ok() {
			code = writable.Chmod(name, srcResult.attr.Mode&07777|0200, context)
		}
		if code.Ok() {
			aTime := srcResult.attr.AccessTime()
			mTime := srcResult.attr.ModTime()
			code = writable.Utimens(name, &aTime, &mTime, context)
		}

		files := fs.nodeFs.AllFiles(name, 0)
		for _, fileWrapper := range files {
			if !code.Ok() {
				break
			}
			var uf *unionFsFile
			f := fileWrapper.File
			for f != nil {
				ok := false
				uf, ok = f.(*unionFsFile)
				if ok {
					break
				}
				f = f.InnerFile()
			}
			if uf == nil {
				panic("no unionFsFile found inside")
			}

			if uf.layer > 0 {
				uf.layer = 0
				f := uf.File
				uf.File, code = fs.fileSystems[0].Open(name, fileWrapper.OpenFlags, context)
				f.Flush()
				f.Release()
			}
		}
	} else if srcResult.attr.IsSymlink() {
		link := ""
		link, code = sourceFs.Readlink(name, context)
		if !code.Ok() {
			log.Println("can't read link in source fs", name)
		} else {
			code = writable.Symlink(link, name, context)
		}
	} else if srcResult.attr.IsDir() {
		code = writable.Mkdir(name, srcResult.attr.Mode&07777|0200, context)
	} else {
		log.Println("Unknown file type:", srcResult.attr)
		return fuse.ENOSYS
	}

	if !code.Ok() {
		fs.branchCache.GetFresh(name)
		return code
	} else {
		r := fs.getBranch(name)
		r.branch = 0
		fs.setBranch(name, r)
	}

	return fuse.OK
}

////////////////////////////////////////////////////////////////
// Below: implement interface for a FileSystem.

func (fs *unionFS) Link(orig string, newName string, context *fuse.Context) (code fuse.Status) {
	origResult := fs.getBranch(orig)
	code = origResult.code
	if code.Ok() && origResult.branch > 0 {
		code = fs.Promote(orig, origResult, context)
	}
	if code.Ok() && origResult.branch > 0 {
		// Hairy: for the link to be hooked up to the existing
		// inode, PathNodeFs must see a client inode for the
		// original.  We force a refresh of the attribute (so
		// the Ino is filled in.), and then force PathNodeFs
		// to see the Inode number.
		fs.branchCache.GetFresh(orig)
		inode := fs.nodeFs.Node(orig)
		var a fuse.Attr
		inode.Node().GetAttr(&a, nil, nil)
	}
	if code.Ok() {
		code = fs.promoteDirsTo(newName)
	}
	if code.Ok() {
		code = fs.fileSystems[0].Link(orig, newName, context)
	}
	if code.Ok() {
		fs.removeDeletion(newName)
		fs.branchCache.GetFresh(newName)
	}
	return code
}

func (fs *unionFS) Rmdir(path string, context *fuse.Context) (code fuse.Status) {
	r := fs.getBranch(path)
	if r.code != fuse.OK {
		return r.code
	}
	if !r.attr.IsDir() {
		return fuse.Status(syscall.ENOTDIR)
	}

	stream, code := fs.OpenDir(path, context)
	found := false
	for _ = range stream {
		found = true
	}
	if found {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	if r.branch > 0 {
		code = fs.putDeletion(path)
		return code
	}
	code = fs.fileSystems[0].Rmdir(path, context)
	if code != fuse.OK {
		return code
	}

	r = fs.branchCache.GetFresh(path).(branchResult)
	if r.branch > 0 {
		code = fs.putDeletion(path)
	}
	return code
}

func (fs *unionFS) Mkdir(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	deleted, code := fs.isDeleted(path)
	if !code.Ok() {
		return code
	}

	if !deleted {
		r := fs.getBranch(path)
		if r.code != fuse.ENOENT {
			return fuse.Status(syscall.EEXIST)
		}
	}

	code = fs.promoteDirsTo(path)
	if code.Ok() {
		code = fs.fileSystems[0].Mkdir(path, mode, context)
	}
	if code.Ok() {
		fs.removeDeletion(path)
		attr := &fuse.Attr{
			Mode: fuse.S_IFDIR | mode,
		}
		fs.setBranch(path, branchResult{attr, fuse.OK, 0})
	}

	var stream []fuse.DirEntry
	stream, code = fs.OpenDir(path, context)
	if code.Ok() {
		// This shouldn't happen, but let's be safe.
		for _, entry := range stream {
			fs.putDeletion(filepath.Join(path, entry.Name))
		}
	}

	return code
}

func (fs *unionFS) Symlink(pointedTo string, linkName string, context *fuse.Context) (code fuse.Status) {
	code = fs.promoteDirsTo(linkName)
	if code.Ok() {
		code = fs.fileSystems[0].Symlink(pointedTo, linkName, context)
	}
	if code.Ok() {
		fs.removeDeletion(linkName)
		fs.branchCache.GetFresh(linkName)
	}
	return code
}

func (fs *unionFS) Truncate(path string, size uint64, context *fuse.Context) (code fuse.Status) {
	if path == _DROP_CACHE {
		return fuse.OK
	}

	r := fs.getBranch(path)
	if r.branch > 0 {
		code = fs.Promote(path, r, context)
		r.branch = 0
	}

	if code.Ok() {
		code = fs.fileSystems[0].Truncate(path, size, context)
	}
	if code.Ok() {
		r.attr.Size = size
		now := time.Now()
		r.attr.SetTimes(nil, &now, &now)
		fs.setBranch(path, r)
	}
	return code
}

func (fs *unionFS) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	name = stripSlash(name)
	r := fs.getBranch(name)

	code = r.code
	if code.Ok() && r.branch > 0 {
		code = fs.Promote(name, r, context)
		r.branch = 0
	}
	if code.Ok() {
		code = fs.fileSystems[0].Utimens(name, atime, mtime, context)
	}
	if code.Ok() {
		now := time.Now()
		r.attr.SetTimes(atime, mtime, &now)
		fs.setBranch(name, r)
	}
	return code
}

func (fs *unionFS) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	name = stripSlash(name)
	r := fs.getBranch(name)
	if r.attr == nil || r.code != fuse.OK {
		return r.code
	}

	if os.Geteuid() != 0 {
		return fuse.EPERM
	}

	if r.attr.Uid != uid || r.attr.Gid != gid {
		if r.branch > 0 {
			code := fs.Promote(name, r, context)
			if code != fuse.OK {
				return code
			}
			r.branch = 0
		}
		fs.fileSystems[0].Chown(name, uid, gid, context)
	}
	r.attr.Uid = uid
	r.attr.Gid = gid
	now := time.Now()
	r.attr.SetTimes(nil, nil, &now)
	fs.setBranch(name, r)
	return fuse.OK
}

func (fs *unionFS) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	name = stripSlash(name)
	r := fs.getBranch(name)
	if r.attr == nil {
		return r.code
	}
	if r.code != fuse.OK {
		return r.code
	}

	permMask := uint32(07777)

	// Always be writable.
	oldMode := r.attr.Mode & permMask

	if oldMode != mode {
		if r.branch > 0 {
			code := fs.Promote(name, r, context)
			if code != fuse.OK {
				return code
			}
			r.branch = 0
		}
		fs.fileSystems[0].Chmod(name, mode, context)
	}
	r.attr.Mode = (r.attr.Mode &^ permMask) | mode
	now := time.Now()
	r.attr.SetTimes(nil, nil, &now)
	fs.setBranch(name, r)
	return fuse.OK
}

func (fs *unionFS) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	// We always allow writing.
	mode = mode &^ fuse.W_OK
	if name == "" || name == _DROP_CACHE {
		return fuse.OK
	}
	r := fs.getBranch(name)
	if r.branch >= 0 {
		return fs.fileSystems[r.branch].Access(name, mode, context)
	}
	return fuse.ENOENT
}

func (fs *unionFS) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	r := fs.getBranch(name)
	if r.branch == 0 {
		code = fs.fileSystems[0].Unlink(name, context)
		if code != fuse.OK {
			return code
		}
		r = fs.branchCache.GetFresh(name).(branchResult)
	}

	if r.branch > 0 {
		// It would be nice to do the putDeletion async.
		code = fs.putDeletion(name)
	}
	return code
}

func (fs *unionFS) Readlink(name string, context *fuse.Context) (out string, code fuse.Status) {
	r := fs.getBranch(name)
	if r.branch >= 0 {
		return fs.fileSystems[r.branch].Readlink(name, context)
	}
	return "", fuse.ENOENT
}

func stripSlash(fn string) string {
	return strings.TrimRight(fn, string(filepath.Separator))
}

func (fs *unionFS) promoteDirsTo(filename string) fuse.Status {
	dirName, _ := filepath.Split(filename)
	dirName = stripSlash(dirName)

	var todo []string
	var results []branchResult
	for dirName != "" {
		r := fs.getBranch(dirName)

		if !r.code.Ok() {
			log.Println("path component does not exist", filename, dirName)
		}
		if !r.attr.IsDir() {
			log.Println("path component is not a directory.", dirName, r)
			return fuse.EPERM
		}
		if r.branch == 0 {
			break
		}
		todo = append(todo, dirName)
		results = append(results, r)
		dirName, _ = filepath.Split(dirName)
		dirName = stripSlash(dirName)
	}

	for i := range todo {
		j := len(todo) - i - 1
		d := todo[j]
		r := results[j]
		code := fs.fileSystems[0].Mkdir(d, r.attr.Mode&07777|0200, nil)
		if code != fuse.OK {
			log.Println("Error creating dir leading to path", d, code, fs.fileSystems[0])
			return fuse.EPERM
		}

		aTime := r.attr.AccessTime()
		mTime := r.attr.ModTime()
		fs.fileSystems[0].Utimens(d, &aTime, &mTime, nil)
		r.branch = 0
		fs.setBranch(d, r)
	}
	return fuse.OK
}

func (fs *unionFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	writable := fs.fileSystems[0]

	code = fs.promoteDirsTo(name)
	if code != fuse.OK {
		return nil, code
	}
	fuseFile, code = writable.Create(name, flags, mode, context)
	if code.Ok() {
		fuseFile = fs.newUnionFsFile(fuseFile, 0)
		fs.removeDeletion(name)

		now := time.Now()
		a := fuse.Attr{
			Mode: fuse.S_IFREG | mode,
		}
		a.SetTimes(nil, &now, &now)
		fs.setBranch(name, branchResult{&a, fuse.OK, 0})
	}
	return fuseFile, code
}

func (fs *unionFS) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, s fuse.Status) {
	_, hidden := fs.hiddenFiles[name]
	if hidden {
		return nil, fuse.ENOENT
	}
	if name == _DROP_CACHE {
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0777,
		}, fuse.OK
	}
	if name == fs.options.DeletionDirName {
		return nil, fuse.ENOENT
	}
	isDel, s := fs.isDeleted(name)
	if !s.Ok() {
		return nil, s
	}

	if isDel {
		return nil, fuse.ENOENT
	}
	r := fs.getBranch(name)
	if r.branch < 0 {
		return nil, fuse.ENOENT
	}
	fi := *r.attr
	// Make everything appear writable.
	fi.Mode |= 0200
	return &fi, r.code
}

func (fs *unionFS) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	if name == _DROP_CACHE {
		return nil, fuse.ENOATTR
	}
	r := fs.getBranch(name)
	if r.branch >= 0 {
		return fs.fileSystems[r.branch].GetXAttr(name, attr, context)
	}
	return nil, fuse.ENOENT
}

func (fs *unionFS) OpenDir(directory string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	dirBranch := fs.getBranch(directory)
	if dirBranch.branch < 0 {
		return nil, fuse.ENOENT
	}

	// We could try to use the cache, but we have a delay, so
	// might as well get the fresh results async.
	var wg sync.WaitGroup
	var deletions map[string]bool

	wg.Add(1)
	go func() {
		deletions = newDirnameMap(fs.fileSystems[0], fs.options.DeletionDirName)
		wg.Done()
	}()

	entries := make([]map[string]uint32, len(fs.fileSystems))
	for i := range fs.fileSystems {
		entries[i] = make(map[string]uint32)
	}

	statuses := make([]fuse.Status, len(fs.fileSystems))
	for i, l := range fs.fileSystems {
		if i >= dirBranch.branch {
			wg.Add(1)
			go func(j int, pfs pathfs.FileSystem) {
				ch, s := pfs.OpenDir(directory, context)
				statuses[j] = s
				for _, v := range ch {
					entries[j][v.Name] = v.Mode
				}
				wg.Done()
			}(i, l)
		}
	}

	wg.Wait()
	if deletions == nil {
		_, code := fs.fileSystems[0].GetAttr(fs.options.DeletionDirName, context)
		if code == fuse.ENOENT {
			deletions = map[string]bool{}
		} else {
			return nil, fuse.Status(syscall.EROFS)
		}
	}

	results := entries[0]

	// TODO(hanwen): should we do anything with the return
	// statuses?
	for i, m := range entries {
		if statuses[i] != fuse.OK {
			continue
		}
		if i == 0 {
			// We don't need to further process the first
			// branch: it has no deleted files.
			continue
		}
		for k, v := range m {
			_, ok := results[k]
			if ok {
				continue
			}

			deleted := deletions[filePathHash(filepath.Join(directory, k))]
			if !deleted {
				results[k] = v
			}
		}
	}
	if directory == "" {
		delete(results, fs.options.DeletionDirName)
		for name, _ := range fs.hiddenFiles {
			delete(results, name)
		}
	}

	stream = make([]fuse.DirEntry, 0, len(results))
	for k, v := range results {
		stream = append(stream, fuse.DirEntry{
			Name: k,
			Mode: v,
		})
	}
	return stream, fuse.OK
}

// recursivePromote promotes path, and if a directory, everything
// below that directory.  It returns a list of all promoted paths, in
// full, including the path itself.
func (fs *unionFS) recursivePromote(path string, pathResult branchResult, context *fuse.Context) (names []string, code fuse.Status) {
	names = []string{}
	if pathResult.branch > 0 {
		code = fs.Promote(path, pathResult, context)
	}

	if code.Ok() {
		names = append(names, path)
	}

	if code.Ok() && pathResult.attr != nil && pathResult.attr.IsDir() {
		var stream []fuse.DirEntry
		stream, code = fs.OpenDir(path, context)
		for _, e := range stream {
			if !code.Ok() {
				break
			}
			subnames := []string{}
			p := filepath.Join(path, e.Name)
			r := fs.getBranch(p)
			subnames, code = fs.recursivePromote(p, r, context)
			names = append(names, subnames...)
		}
	}

	if !code.Ok() {
		names = nil
	}
	return names, code
}

func (fs *unionFS) renameDirectory(srcResult branchResult, srcDir string, dstDir string, context *fuse.Context) (code fuse.Status) {
	names := []string{}
	if code.Ok() {
		names, code = fs.recursivePromote(srcDir, srcResult, context)
	}
	if code.Ok() {
		code = fs.promoteDirsTo(dstDir)
	}

	if code.Ok() {
		writable := fs.fileSystems[0]
		code = writable.Rename(srcDir, dstDir, context)
	}

	if code.Ok() {
		for _, srcName := range names {
			relative := strings.TrimLeft(srcName[len(srcDir):], string(filepath.Separator))
			dst := filepath.Join(dstDir, relative)
			fs.removeDeletion(dst)

			srcResult := fs.getBranch(srcName)
			srcResult.branch = 0
			fs.setBranch(dst, srcResult)

			srcResult = fs.branchCache.GetFresh(srcName).(branchResult)
			if srcResult.branch > 0 {
				code = fs.putDeletion(srcName)
			}
		}
	}
	return code
}

func (fs *unionFS) Rename(src string, dst string, context *fuse.Context) (code fuse.Status) {
	srcResult := fs.getBranch(src)
	code = srcResult.code
	if code.Ok() {
		code = srcResult.code
	}

	if srcResult.attr.IsDir() {
		return fs.renameDirectory(srcResult, src, dst, context)
	}

	if code.Ok() && srcResult.branch > 0 {
		code = fs.Promote(src, srcResult, context)
	}
	if code.Ok() {
		code = fs.promoteDirsTo(dst)
	}
	if code.Ok() {
		code = fs.fileSystems[0].Rename(src, dst, context)
	}

	if code.Ok() {
		fs.removeDeletion(dst)
		// Rename is racy; avoid racing with unionFsFile.Release().
		fs.branchCache.DropEntry(dst)

		srcResult := fs.branchCache.GetFresh(src)
		if srcResult.(branchResult).branch > 0 {
			code = fs.putDeletion(src)
		}
	}
	return code
}

func (fs *unionFS) DropBranchCache(names []string) {
	fs.branchCache.DropAll(names)
}

func (fs *unionFS) DropDeletionCache() {
	fs.deletionCache.DropCache()
}

func (fs *unionFS) DropSubFsCaches() {
	for _, fs := range fs.fileSystems {
		a, code := fs.GetAttr(_DROP_CACHE, nil)
		if code.Ok() && a.IsRegular() {
			f, _ := fs.Open(_DROP_CACHE, uint32(os.O_WRONLY), nil)
			if f != nil {
				f.Flush()
				f.Release()
			}
		}
	}
}

func (fs *unionFS) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	if name == _DROP_CACHE {
		if flags&fuse.O_ANYWRITE != 0 {
			log.Println("Forced cache drop on", fs)
			fs.DropBranchCache(nil)
			fs.DropDeletionCache()
			fs.DropSubFsCaches()
			fs.nodeFs.ForgetClientInodes()
		}
		return nodefs.NewDevNullFile(), fuse.OK
	}
	r := fs.getBranch(name)
	if r.branch < 0 {
		// This should not happen, as a GetAttr() should have
		// already verified existence.
		log.Println("UnionFs: open of non-existent file:", name)
		return nil, fuse.ENOENT
	}
	if flags&fuse.O_ANYWRITE != 0 && r.branch > 0 {
		code := fs.Promote(name, r, context)
		if code != fuse.OK {
			return nil, code
		}
		r.branch = 0
		now := time.Now()
		r.attr.SetTimes(nil, &now, nil)
		fs.setBranch(name, r)
	}
	fuseFile, status = fs.fileSystems[r.branch].Open(name, uint32(flags), context)
	if fuseFile != nil {
		fuseFile = fs.newUnionFsFile(fuseFile, r.branch)
	}
	return fuseFile, status
}

func (fs *unionFS) String() string {
	names := []string{}
	for _, fs := range fs.fileSystems {
		names = append(names, fs.String())
	}
	return fmt.Sprintf("UnionFs(%v)", names)
}

func (fs *unionFS) StatFs(name string) *fuse.StatfsOut {
	return fs.fileSystems[0].StatFs("")
}

type unionFsFile struct {
	nodefs.File
	ufs   *unionFS
	node  *nodefs.Inode
	layer int
}

func (fs *unionFsFile) String() string {
	return fmt.Sprintf("unionFsFile(%s)", fs.File.String())
}

func (fs *unionFS) newUnionFsFile(f nodefs.File, branch int) *unionFsFile {
	return &unionFsFile{
		File:  f,
		ufs:   fs,
		layer: branch,
	}
}

func (fs *unionFsFile) InnerFile() (file nodefs.File) {
	return fs.File
}

// We can't hook on Release. Release has no response, so it is not
// ordered wrt any following calls.
func (fs *unionFsFile) Flush() (code fuse.Status) {
	code = fs.File.Flush()
	path := fs.ufs.nodeFs.Path(fs.node)
	fs.ufs.branchCache.GetFresh(path)
	return code
}

func (fs *unionFsFile) SetInode(node *nodefs.Inode) {
	fs.node = node
}

func (fs *unionFsFile) GetAttr(out *fuse.Attr) fuse.Status {
	code := fs.File.GetAttr(out)
	if code.Ok() {
		out.Mode |= 0200
	}
	return code
}
