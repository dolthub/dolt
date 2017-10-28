package mfs

import (
	"errors"
	"fmt"
	"os"
	gopath "path"
	"strings"

	path "github.com/ipfs/go-ipfs/path"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
)

// Mv moves the file or directory at 'src' to 'dst'
func Mv(r *Root, src, dst string) error {
	srcDir, srcFname := gopath.Split(src)

	var dstDirStr string
	var filename string
	if dst[len(dst)-1] == '/' {
		dstDirStr = dst
		filename = srcFname
	} else {
		dstDirStr, filename = gopath.Split(dst)
	}

	// get parent directories of both src and dest first
	dstDir, err := lookupDir(r, dstDirStr)
	if err != nil {
		return err
	}

	srcDirObj, err := lookupDir(r, srcDir)
	if err != nil {
		return err
	}

	srcObj, err := srcDirObj.Child(srcFname)
	if err != nil {
		return err
	}

	nd, err := srcObj.GetNode()
	if err != nil {
		return err
	}

	fsn, err := dstDir.Child(filename)
	if err == nil {
		switch n := fsn.(type) {
		case *File:
			_ = dstDir.Unlink(filename)
		case *Directory:
			dstDir = n
		default:
			return fmt.Errorf("unexpected type at path: %s", dst)
		}
	} else if err != os.ErrNotExist {
		return err
	}

	err = dstDir.AddChild(filename, nd)
	if err != nil {
		return err
	}

	return srcDirObj.Unlink(srcFname)
}

func lookupDir(r *Root, path string) (*Directory, error) {
	di, err := Lookup(r, path)
	if err != nil {
		return nil, err
	}

	d, ok := di.(*Directory)
	if !ok {
		return nil, fmt.Errorf("%s is not a directory", path)
	}

	return d, nil
}

// PutNode inserts 'nd' at 'path' in the given mfs
func PutNode(r *Root, path string, nd node.Node) error {
	dirp, filename := gopath.Split(path)
	if filename == "" {
		return fmt.Errorf("cannot create file with empty name")
	}

	pdir, err := lookupDir(r, dirp)
	if err != nil {
		return err
	}

	return pdir.AddChild(filename, nd)
}

// MkdirOpts is used by Mkdir
type MkdirOpts struct {
	Mkparents bool
	Flush     bool
	Prefix    *cid.Prefix
}

// Mkdir creates a directory at 'path' under the directory 'd', creating
// intermediary directories as needed if 'mkparents' is set to true
func Mkdir(r *Root, pth string, opts MkdirOpts) error {
	if pth == "" {
		return fmt.Errorf("no path given to Mkdir")
	}
	parts := path.SplitList(pth)
	if parts[0] == "" {
		parts = parts[1:]
	}

	// allow 'mkdir /a/b/c/' to create c
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}

	if len(parts) == 0 {
		// this will only happen on 'mkdir /'
		if opts.Mkparents {
			return nil
		}
		return fmt.Errorf("cannot create directory '/': Already exists")
	}

	cur := r.GetValue().(*Directory)
	for i, d := range parts[:len(parts)-1] {
		fsn, err := cur.Child(d)
		if err == os.ErrNotExist && opts.Mkparents {
			mkd, err := cur.Mkdir(d)
			if err != nil {
				return err
			}
			if opts.Prefix != nil {
				mkd.SetPrefix(opts.Prefix)
			}
			fsn = mkd
		} else if err != nil {
			return err
		}

		next, ok := fsn.(*Directory)
		if !ok {
			return fmt.Errorf("%s was not a directory", path.Join(parts[:i]))
		}
		cur = next
	}

	final, err := cur.Mkdir(parts[len(parts)-1])
	if err != nil {
		if !opts.Mkparents || err != os.ErrExist || final == nil {
			return err
		}
	}
	if opts.Prefix != nil {
		final.SetPrefix(opts.Prefix)
	}

	if opts.Flush {
		err := final.Flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func Lookup(r *Root, path string) (FSNode, error) {
	dir, ok := r.GetValue().(*Directory)
	if !ok {
		log.Errorf("root not a dir: %#v", r.GetValue())
		return nil, errors.New("root was not a directory")
	}

	return DirLookup(dir, path)
}

// DirLookup will look up a file or directory at the given path
// under the directory 'd'
func DirLookup(d *Directory, pth string) (FSNode, error) {
	pth = strings.Trim(pth, "/")
	parts := path.SplitList(pth)
	if len(parts) == 1 && parts[0] == "" {
		return d, nil
	}

	var cur FSNode
	cur = d
	for i, p := range parts {
		chdir, ok := cur.(*Directory)
		if !ok {
			return nil, fmt.Errorf("cannot access %s: Not a directory", path.Join(parts[:i+1]))
		}

		child, err := chdir.Child(p)
		if err != nil {
			return nil, err
		}

		cur = child
	}
	return cur, nil
}

func FlushPath(rt *Root, pth string) error {
	nd, err := Lookup(rt, pth)
	if err != nil {
		return err
	}

	err = nd.Flush()
	if err != nil {
		return err
	}

	rt.repub.WaitPub()
	return nil
}
