package commands

import (
	"context"
	"fmt"
	"io"

	cmds "github.com/ipfs/go-ipfs/commands"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/filestore"
	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

var FileStoreCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Interact with filestore objects.",
	},
	Subcommands: map[string]*cmds.Command{
		"ls":     lsFileStore,
		"verify": verifyFileStore,
		"dups":   dupsFileStore,
	},
}

var lsFileStore = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List objects in filestore.",
		LongDescription: `
List objects in the filestore.

If one or more <obj> is specified only list those specific objects,
otherwise list all objects.

The output is:

<hash> <size> <path> <offset>
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("obj", false, true, "Cid of objects to list."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("file-order", "sort the results based on the path of the backing file"),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		_, fs, err := getFilestore(req)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		args := req.Arguments()
		if len(args) > 0 {
			out := perKeyActionToChan(args, func(c *cid.Cid) *filestore.ListRes {
				return filestore.List(fs, c)
			}, req.Context())
			res.SetOutput(out)
		} else {
			fileOrder, _, _ := req.Option("file-order").Bool()
			next, err := filestore.ListAll(fs, fileOrder)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
			out := listResToChan(next, req.Context())
			res.SetOutput(out)
		}
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			outChan, ok := res.Output().(<-chan interface{})
			if !ok {
				return nil, u.ErrCast()
			}
			errors := false
			for r0 := range outChan {
				r := r0.(*filestore.ListRes)
				if r.ErrorMsg != "" {
					errors = true
					fmt.Fprintf(res.Stderr(), "%s\n", r.ErrorMsg)
				} else {
					fmt.Fprintf(res.Stdout(), "%s\n", r.FormatLong())
				}
			}
			if errors {
				return nil, fmt.Errorf("errors while displaying some entries")
			}
			return nil, nil
		},
	},
	Type: filestore.ListRes{},
}

var verifyFileStore = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Verify objects in filestore.",
		LongDescription: `
Verify objects in the filestore.

If one or more <obj> is specified only verify those specific objects,
otherwise verify all objects.

The output is:

<status> <hash> <size> <path> <offset>

Where <status> is one of:
ok:       the block can be reconstructed
changed:  the contents of the backing file have changed
no-file:  the backing file could not be found
error:    there was some other problem reading the file
missing:  <obj> could not be found in the filestore
ERROR:    internal error, most likely due to a corrupt database

For ERROR entries the error will also be printed to stderr.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("obj", false, true, "Cid of objects to verify."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("file-order", "verify the objects based on the order of the backing file"),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		_, fs, err := getFilestore(req)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		args := req.Arguments()
		if len(args) > 0 {
			out := perKeyActionToChan(args, func(c *cid.Cid) *filestore.ListRes {
				return filestore.Verify(fs, c)
			}, req.Context())
			res.SetOutput(out)
		} else {
			fileOrder, _, _ := req.Option("file-order").Bool()
			next, err := filestore.VerifyAll(fs, fileOrder)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
			out := listResToChan(next, req.Context())
			res.SetOutput(out)
		}
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			outChan, ok := res.Output().(<-chan interface{})
			if !ok {
				return nil, u.ErrCast()
			}
			res.SetOutput(nil)
			for r0 := range outChan {
				r := r0.(*filestore.ListRes)
				if r.Status == filestore.StatusOtherError {
					fmt.Fprintf(res.Stderr(), "%s\n", r.ErrorMsg)
				}
				fmt.Fprintf(res.Stdout(), "%s %s\n", r.Status.Format(), r.FormatLong())
			}
			return nil, nil
		},
	},
	Type: filestore.ListRes{},
}

var dupsFileStore = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List blocks that are both in the filestore and standard block storage.",
	},
	Run: func(req cmds.Request, res cmds.Response) {
		_, fs, err := getFilestore(req)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		ch, err := fs.FileManager().AllKeysChan(req.Context())
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		out := make(chan interface{}, 128)
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer close(out)
			for cid := range ch {
				have, err := fs.MainBlockstore().Has(cid)
				if err != nil {
					out <- &RefWrapper{Err: err.Error()}
					return
				}
				if have {
					out <- &RefWrapper{Ref: cid.String()}
				}
			}
		}()
	},
	Marshalers: refsMarshallerMap,
	Type:       RefWrapper{},
}

func getFilestore(req cmds.Request) (*core.IpfsNode, *filestore.Filestore, error) {
	n, err := req.InvocContext().GetNode()
	if err != nil {
		return nil, nil, err
	}
	fs := n.Filestore
	if fs == nil {
		return n, nil, fmt.Errorf("filestore not enabled")
	}
	return n, fs, err
}

func listResToChan(next func() *filestore.ListRes, ctx context.Context) <-chan interface{} {
	out := make(chan interface{}, 128)
	go func() {
		defer close(out)
		for {
			r := next()
			if r == nil {
				return
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func perKeyActionToChan(args []string, action func(*cid.Cid) *filestore.ListRes, ctx context.Context) <-chan interface{} {
	out := make(chan interface{}, 128)
	go func() {
		defer close(out)
		for _, arg := range args {
			c, err := cid.Decode(arg)
			if err != nil {
				out <- &filestore.ListRes{
					Status:   filestore.StatusOtherError,
					ErrorMsg: fmt.Sprintf("%s: %v", arg, err),
				}
				continue
			}
			r := action(c)
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
