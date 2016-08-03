package unionfs

import (
	"os"

	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func NewUnionFsFromRoots(roots []string, opts *UnionFsOptions, roCaching bool) (pathfs.FileSystem, error) {
	fses := make([]pathfs.FileSystem, 0)
	for i, r := range roots {
		var fs pathfs.FileSystem
		fi, err := os.Stat(r)
		if err != nil {
			return nil, err
		}
		if fi.IsDir() {
			fs = pathfs.NewLoopbackFileSystem(r)
		}
		if fs == nil {
			return nil, err

		}
		if i > 0 && roCaching {
			fs = NewCachingFileSystem(fs, 0)
		}

		fses = append(fses, fs)
	}

	return NewUnionFs(fses, *opts)
}
