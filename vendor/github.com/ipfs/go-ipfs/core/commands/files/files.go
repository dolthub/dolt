package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	gopath "path"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	dag "github.com/ipfs/go-ipfs/merkledag"
	mfs "github.com/ipfs/go-ipfs/mfs"
	path "github.com/ipfs/go-ipfs/path"
	ft "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	node "gx/ipfs/QmYNyRZJBUYPNrLszFmrBrPJbsBh2vMsefz5gnDpB5M1P6/go-ipld-format"
)

var log = logging.Logger("cmds/files")

var FilesCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Interact with unixfs files.",
		ShortDescription: `
Files is an API for manipulating IPFS objects as if they were a unix
filesystem.

NOTE:
Most of the subcommands of 'ipfs files' accept the '--flush' flag. It defaults
to true. Use caution when setting this flag to false. It will improve
performance for large numbers of file operations, but it does so at the cost
of consistency guarantees. If the daemon is unexpectedly killed before running
'ipfs files flush' on the files in question, then data may be lost. This also
applies to running 'ipfs repo gc' concurrently with '--flush=false'
operations.
`,
	},
	Options: []cmds.Option{
		cmds.BoolOption("f", "flush", "Flush target and ancestors after write.").Default(true),
	},
	Subcommands: map[string]*cmds.Command{
		"read":  FilesReadCmd,
		"write": FilesWriteCmd,
		"mv":    FilesMvCmd,
		"cp":    FilesCpCmd,
		"ls":    FilesLsCmd,
		"mkdir": FilesMkdirCmd,
		"stat":  FilesStatCmd,
		"rm":    FilesRmCmd,
		"flush": FilesFlushCmd,
	},
}

var formatError = errors.New("Format was set by multiple options. Only one format option is allowed")

var FilesStatCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Display file status.",
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("path", true, false, "Path to node to stat."),
	},
	Options: []cmds.Option{
		cmds.StringOption("format", "Print statistics in given format. Allowed tokens: "+
			"<hash> <size> <cumulsize> <type> <childs>. Conflicts with other format options.").Default(
			`<hash>
Size: <size>
CumulativeSize: <cumulsize>
ChildBlocks: <childs>
Type: <type>`),
		cmds.BoolOption("hash", "Print only hash. Implies '--format=<hash>'. Conflicts with other format options.").Default(false),
		cmds.BoolOption("size", "Print only size. Implies '--format=<cumulsize>'. Conflicts with other format options.").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {

		_, err := statGetFormatOptions(req)
		if err != nil {
			res.SetError(err, cmds.ErrClient)
		}

		node, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		path, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		fsn, err := mfs.Lookup(node.FilesRoot, path)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		o, err := statNode(node.DAG, fsn)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(o)
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {

			out := res.Output().(*Object)
			buf := new(bytes.Buffer)

			s, _ := statGetFormatOptions(res.Request())
			s = strings.Replace(s, "<hash>", out.Hash, -1)
			s = strings.Replace(s, "<size>", fmt.Sprintf("%d", out.Size), -1)
			s = strings.Replace(s, "<cumulsize>", fmt.Sprintf("%d", out.CumulativeSize), -1)
			s = strings.Replace(s, "<childs>", fmt.Sprintf("%d", out.Blocks), -1)
			s = strings.Replace(s, "<type>", out.Type, -1)

			fmt.Fprintln(buf, s)
			return buf, nil
		},
	},
	Type: Object{},
}

func moreThanOne(a, b, c bool) bool {
	return a && b || b && c || a && c
}

func statGetFormatOptions(req cmds.Request) (string, error) {

	hash, _, _ := req.Option("hash").Bool()
	size, _, _ := req.Option("size").Bool()
	format, found, _ := req.Option("format").String()

	if moreThanOne(hash, size, found) {
		return "", formatError
	}

	if hash {
		return "<hash>", nil
	} else if size {
		return "<cumulsize>", nil
	} else {
		return format, nil
	}
}

func statNode(ds dag.DAGService, fsn mfs.FSNode) (*Object, error) {
	nd, err := fsn.GetNode()
	if err != nil {
		return nil, err
	}

	c := nd.Cid()

	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	d, err := ft.FromBytes(pbnd.Data())
	if err != nil {
		return nil, err
	}

	cumulsize, err := nd.Size()
	if err != nil {
		return nil, err
	}

	var ndtype string
	switch fsn.Type() {
	case mfs.TDir:
		ndtype = "directory"
	case mfs.TFile:
		ndtype = "file"
	default:
		return nil, fmt.Errorf("Unrecognized node type: %s", fsn.Type())
	}

	return &Object{
		Hash:           c.String(),
		Blocks:         len(nd.Links()),
		Size:           d.GetFilesize(),
		CumulativeSize: cumulsize,
		Type:           ndtype,
	}, nil
}

var FilesCpCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Copy files into mfs.",
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("source", true, false, "Source object to copy."),
		cmds.StringArg("dest", true, false, "Destination to copy object to."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		node, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		flush, _, _ := req.Option("flush").Bool()

		src, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		src = strings.TrimRight(src, "/")

		dst, err := checkPath(req.Arguments()[1])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if dst[len(dst)-1] == '/' {
			dst += gopath.Base(src)
		}

		nd, err := getNodeFromPath(req.Context(), node, src)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = mfs.PutNode(node.FilesRoot, dst, nd)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if flush {
			err := mfs.FlushPath(node.FilesRoot, dst)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}
	},
}

func getNodeFromPath(ctx context.Context, node *core.IpfsNode, p string) (node.Node, error) {
	switch {
	case strings.HasPrefix(p, "/ipfs/"):
		np, err := path.ParsePath(p)
		if err != nil {
			return nil, err
		}

		resolver := &path.Resolver{
			DAG:         node.DAG,
			ResolveOnce: uio.ResolveUnixfsOnce,
		}

		return core.Resolve(ctx, node.Namesys, resolver, np)
	default:
		fsn, err := mfs.Lookup(node.FilesRoot, p)
		if err != nil {
			return nil, err
		}

		return fsn.GetNode()
	}
}

type Object struct {
	Hash           string
	Size           uint64
	CumulativeSize uint64
	Blocks         int
	Type           string
}

type FilesLsOutput struct {
	Entries []mfs.NodeListing
}

var FilesLsCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List directories in the local mutable namespace.",
		ShortDescription: `
List directories in the local mutable namespace.

Examples:

    $ ipfs files ls /welcome/docs/
    about
    contact
    help
    quick-start
    readme
    security-notes

    $ ipfs files ls /myfiles/a/b/c/d
    foo
    bar
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("path", false, false, "Path to show listing for. Defaults to '/'."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("l", "Use long listing format."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		var arg string

		if len(req.Arguments()) == 0 {
			arg = "/"
		} else {
			arg = req.Arguments()[0]
		}

		path, err := checkPath(arg)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		fsn, err := mfs.Lookup(nd.FilesRoot, path)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		long, _, _ := req.Option("l").Bool()

		switch fsn := fsn.(type) {
		case *mfs.Directory:
			if !long {
				var output []mfs.NodeListing
				names, err := fsn.ListNames(req.Context())
				if err != nil {
					res.SetError(err, cmds.ErrNormal)
					return
				}

				for _, name := range names {
					output = append(output, mfs.NodeListing{
						Name: name,
					})
				}
				res.SetOutput(&FilesLsOutput{output})
			} else {
				listing, err := fsn.List(req.Context())
				if err != nil {
					res.SetError(err, cmds.ErrNormal)
					return
				}
				res.SetOutput(&FilesLsOutput{listing})
			}
			return
		case *mfs.File:
			_, name := gopath.Split(path)
			out := &FilesLsOutput{[]mfs.NodeListing{mfs.NodeListing{Name: name, Type: 1}}}
			res.SetOutput(out)
			return
		default:
			res.SetError(errors.New("unrecognized type"), cmds.ErrNormal)
		}
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			out := res.Output().(*FilesLsOutput)
			buf := new(bytes.Buffer)
			long, _, _ := res.Request().Option("l").Bool()

			for _, o := range out.Entries {
				if long {
					fmt.Fprintf(buf, "%s\t%s\t%d\n", o.Name, o.Hash, o.Size)
				} else {
					fmt.Fprintf(buf, "%s\n", o.Name)
				}
			}
			return buf, nil
		},
	},
	Type: FilesLsOutput{},
}

var FilesReadCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Read a file in a given mfs.",
		ShortDescription: `
Read a specified number of bytes from a file at a given offset. By default,
will read the entire file similar to unix cat.

Examples:

    $ ipfs files read /test/hello
    hello
        `,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("path", true, false, "Path to file to be read."),
	},
	Options: []cmds.Option{
		cmds.IntOption("offset", "o", "Byte offset to begin reading from."),
		cmds.IntOption("count", "n", "Maximum number of bytes to read."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		path, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		fsn, err := mfs.Lookup(n.FilesRoot, path)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		fi, ok := fsn.(*mfs.File)
		if !ok {
			res.SetError(fmt.Errorf("%s was not a file.", path), cmds.ErrNormal)
			return
		}

		rfd, err := fi.Open(mfs.OpenReadOnly, false)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		defer rfd.Close()

		offset, _, err := req.Option("offset").Int()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if offset < 0 {
			res.SetError(fmt.Errorf("Cannot specify negative offset."), cmds.ErrNormal)
			return
		}

		filen, err := rfd.Size()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if int64(offset) > filen {
			res.SetError(fmt.Errorf("Offset was past end of file (%d > %d).", offset, filen), cmds.ErrNormal)
			return
		}

		_, err = rfd.Seek(int64(offset), io.SeekStart)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		var r io.Reader = &contextReaderWrapper{R: rfd, ctx: req.Context()}
		count, found, err := req.Option("count").Int()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if found {
			if count < 0 {
				res.SetError(fmt.Errorf("Cannot specify negative 'count'."), cmds.ErrNormal)
				return
			}
			r = io.LimitReader(r, int64(count))
		}

		res.SetOutput(r)
	},
}

type contextReader interface {
	CtxReadFull(context.Context, []byte) (int, error)
}

type contextReaderWrapper struct {
	R   contextReader
	ctx context.Context
}

func (crw *contextReaderWrapper) Read(b []byte) (int, error) {
	return crw.R.CtxReadFull(crw.ctx, b)
}

var FilesMvCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Move files.",
		ShortDescription: `
Move files around. Just like traditional unix mv.

Example:

    $ ipfs files mv /myfs/a/b/c /myfs/foo/newc

`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("source", true, false, "Source file to move."),
		cmds.StringArg("dest", true, false, "Destination path for file to be moved to."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		src, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		dst, err := checkPath(req.Arguments()[1])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		err = mfs.Mv(n.FilesRoot, src, dst)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
	},
}

var FilesWriteCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Write to a mutable file in a given filesystem.",
		ShortDescription: `
Write data to a file in a given filesystem. This command allows you to specify
a beginning offset to write to. The entire length of the input will be written.

If the '--create' option is specified, the file will be created if it does not
exist. Nonexistant intermediate directories will not be created.

If the '--flush' option is set to false, changes will not be propogated to the
merkledag root. This can make operations much faster when doing a large number
of writes to a deeper directory structure.

EXAMPLE:

    echo "hello world" | ipfs files write --create /myfs/a/b/file
    echo "hello world" | ipfs files write --truncate /myfs/a/b/file

WARNING:

Usage of the '--flush=false' option does not guarantee data durability until
the tree has been flushed. This can be accomplished by running 'ipfs files
stat' on the file or any of its ancestors.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("path", true, false, "Path to write to."),
		cmds.FileArg("data", true, false, "Data to write.").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.IntOption("offset", "o", "Byte offset to begin writing at."),
		cmds.BoolOption("create", "e", "Create the file if it does not exist."),
		cmds.BoolOption("truncate", "t", "Truncate the file to size zero before writing."),
		cmds.IntOption("count", "n", "Maximum number of bytes to read."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		path, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		create, _, _ := req.Option("create").Bool()
		trunc, _, _ := req.Option("truncate").Bool()
		flush, _, _ := req.Option("flush").Bool()

		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		offset, _, err := req.Option("offset").Int()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if offset < 0 {
			res.SetError(fmt.Errorf("cannot have negative write offset"), cmds.ErrNormal)
			return
		}

		fi, err := getFileHandle(nd.FilesRoot, path, create)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		wfd, err := fi.Open(mfs.OpenWriteOnly, flush)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		defer func() {
			err := wfd.Close()
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
			}
		}()

		if trunc {
			if err := wfd.Truncate(0); err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}

		count, countfound, err := req.Option("count").Int()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if countfound && count < 0 {
			res.SetError(fmt.Errorf("cannot have negative byte count"), cmds.ErrNormal)
			return
		}

		_, err = wfd.Seek(int64(offset), io.SeekStart)
		if err != nil {
			log.Error("seekfail: ", err)
			res.SetError(err, cmds.ErrNormal)
			return
		}

		input, err := req.Files().NextFile()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		var r io.Reader = input
		if countfound {
			r = io.LimitReader(r, int64(count))
		}

		n, err := io.Copy(wfd, r)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		log.Debugf("wrote %d bytes to %s", n, path)
	},
}

var FilesMkdirCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Make directories.",
		ShortDescription: `
Create the directory if it does not already exist.

NOTE: All paths must be absolute.

Examples:

    $ ipfs mfs mkdir /test/newdir
    $ ipfs mfs mkdir -p /test/does/not/exist/yet
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("path", true, false, "Path to dir to make."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("parents", "p", "No error if existing, make parent directories as needed."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		dashp, _, _ := req.Option("parents").Bool()
		dirtomake, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		flush, _, _ := req.Option("flush").Bool()

		err = mfs.Mkdir(n.FilesRoot, dirtomake, dashp, flush)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

	},
}

var FilesFlushCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Flush a given path's data to disk.",
		ShortDescription: `
Flush a given path to disk. This is only useful when other commands
are run with the '--flush=false'.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("path", false, false, "Path to flush. Default: '/'."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		path := "/"
		if len(req.Arguments()) > 0 {
			path = req.Arguments()[0]
		}

		err = mfs.FlushPath(nd.FilesRoot, path)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
	},
}

var FilesRmCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Remove a file.",
		ShortDescription: `
Remove files or directories.

    $ ipfs files rm /foo
    $ ipfs files ls /bar
    cat
    dog
    fish
    $ ipfs files rm -r /bar
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("path", true, true, "File to remove."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("recursive", "r", "Recursively remove directories."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		path, err := checkPath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if path == "/" {
			res.SetError(fmt.Errorf("cannot delete root"), cmds.ErrNormal)
			return
		}

		// 'rm a/b/c/' will fail unless we trim the slash at the end
		if path[len(path)-1] == '/' {
			path = path[:len(path)-1]
		}

		dir, name := gopath.Split(path)
		parent, err := mfs.Lookup(nd.FilesRoot, dir)
		if err != nil {
			res.SetError(fmt.Errorf("parent lookup: %s", err), cmds.ErrNormal)
			return
		}

		pdir, ok := parent.(*mfs.Directory)
		if !ok {
			res.SetError(fmt.Errorf("No such file or directory: %s", path), cmds.ErrNormal)
			return
		}

		dashr, _, _ := req.Option("r").Bool()

		var success bool
		defer func() {
			if success {
				err := pdir.Flush()
				if err != nil {
					res.SetError(err, cmds.ErrNormal)
					return
				}
			}
		}()

		// if '-r' specified, don't check file type (in bad scenarios, the block may not exist)
		if dashr {
			err := pdir.Unlink(name)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			success = true
			return
		}

		childi, err := pdir.Child(name)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		switch childi.(type) {
		case *mfs.Directory:
			res.SetError(fmt.Errorf("%s is a directory, use -r to remove directories", path), cmds.ErrNormal)
			return
		default:
			err := pdir.Unlink(name)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			success = true
		}
	},
}

func getFileHandle(r *mfs.Root, path string, create bool) (*mfs.File, error) {

	target, err := mfs.Lookup(r, path)
	switch err {
	case nil:
		fi, ok := target.(*mfs.File)
		if !ok {
			return nil, fmt.Errorf("%s was not a file", path)
		}
		return fi, nil

	case os.ErrNotExist:
		if !create {
			return nil, err
		}

		// if create is specified and the file doesnt exist, we create the file
		dirname, fname := gopath.Split(path)
		pdiri, err := mfs.Lookup(r, dirname)
		if err != nil {
			log.Error("lookupfail ", dirname)
			return nil, err
		}
		pdir, ok := pdiri.(*mfs.Directory)
		if !ok {
			return nil, fmt.Errorf("%s was not a directory", dirname)
		}

		nd := dag.NodeWithData(ft.FilePBData(nil, 0))
		err = pdir.AddChild(fname, nd)
		if err != nil {
			return nil, err
		}

		fsn, err := pdir.Child(fname)
		if err != nil {
			return nil, err
		}

		fi, ok := fsn.(*mfs.File)
		if !ok {
			return nil, errors.New("Expected *mfs.File, didnt get it. This is likely a race condition.")
		}
		return fi, nil

	default:
		return nil, err
	}
}

func checkPath(p string) (string, error) {
	if len(p) == 0 {
		return "", fmt.Errorf("Paths must not be empty.")
	}

	if p[0] != '/' {
		return "", fmt.Errorf("Paths must start with a leading slash.")
	}

	cleaned := gopath.Clean(p)
	if p[len(p)-1] == '/' && p != "/" {
		cleaned += "/"
	}
	return cleaned, nil
}
