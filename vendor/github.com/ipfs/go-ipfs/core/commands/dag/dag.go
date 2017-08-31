package dagcmd

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	coredag "github.com/ipfs/go-ipfs/core/coredag"
	path "github.com/ipfs/go-ipfs/path"
	pin "github.com/ipfs/go-ipfs/pin"

	cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
	mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
)

var DagCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Interact with ipld dag objects.",
		ShortDescription: `
'ipfs dag' is used for creating and manipulating dag objects.

This subcommand is currently an experimental feature, but it is intended
to deprecate and replace the existing 'ipfs object' command moving forward.
		`,
	},
	Subcommands: map[string]*cmds.Command{
		"put":     DagPutCmd,
		"get":     DagGetCmd,
		"resolve": DagResolveCmd,
	},
}

// OutputObject is the output type of 'dag put' command
type OutputObject struct {
	Cid *cid.Cid
}

// ResolveOutput is the output type of 'dag resolve' command
type ResolveOutput struct {
	Cid     *cid.Cid
	RemPath string
}

var DagPutCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Add a dag node to ipfs.",
		ShortDescription: `
'ipfs dag put' accepts input from a file or stdin and parses it
into an object of the specified format.
`,
	},
	Arguments: []cmds.Argument{
		cmds.FileArg("object data", true, false, "The object to put").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.StringOption("format", "f", "Format that the object will be added as.").Default("cbor"),
		cmds.StringOption("input-enc", "Format that the input object will be.").Default("json"),
		cmds.BoolOption("pin", "Pin this object when adding.").Default(false),
		cmds.StringOption("hash", "Hash function to use").Default(""),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		fi, err := req.Files().NextFile()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		ienc, _, _ := req.Option("input-enc").String()
		format, _, _ := req.Option("format").String()
		hash, _, err := req.Option("hash").String()
		dopin, _, err := req.Option("pin").Bool()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		// mhType tells inputParser which hash should be used. MaxUint64 means 'use
		// default hash' (sha256 for cbor, sha1 for git..)
		mhType := uint64(math.MaxUint64)

		if hash != "" {
			var ok bool
			mhType, ok = mh.Names[hash]
			if !ok {
				res.SetError(fmt.Errorf("%s in not a valid multihash name", hash), cmds.ErrNormal)
				return
			}
		}

		if dopin {
			defer n.Blockstore.PinLock().Unlock()
		}

		nds, err := coredag.ParseInputs(ienc, format, fi, mhType, -1)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if len(nds) == 0 {
			res.SetError(fmt.Errorf("no node returned from ParseInputs"), cmds.ErrNormal)
			return
		}

		b := n.DAG.Batch()
		for _, nd := range nds {
			_, err := b.Add(nd)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}

		if err := b.Commit(); err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		root := nds[0].Cid()
		if dopin {
			n.Pinning.PinWithMode(root, pin.Recursive)

			err := n.Pinning.Flush()
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}

		res.SetOutput(&OutputObject{Cid: root})
	},
	Type: OutputObject{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			oobj, ok := res.Output().(*OutputObject)
			if !ok {
				return nil, fmt.Errorf("expected a different object in marshaler")
			}

			return strings.NewReader(oobj.Cid.String()), nil
		},
	},
}

var DagGetCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Get a dag node from ipfs.",
		ShortDescription: `
'ipfs dag get' fetches a dag node from ipfs and prints it out in the specifed format.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("ref", true, false, "The object to get").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		p, err := path.ParsePath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		obj, rem, err := n.Resolver.ResolveToLastNode(req.Context(), p)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		var out interface{} = obj
		if len(rem) > 0 {
			final, _, err := obj.Resolve(rem)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
			out = final
		}

		res.SetOutput(out)
	},
}

// DagResolveCmd returns address of highest block within a path and a path remainder
var DagResolveCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Resolve ipld block",
		ShortDescription: `
'ipfs dag resolve' fetches a dag node from ipfs, prints it's address and remaining path.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("ref", true, false, "The path to resolve").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		p, err := path.ParsePath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		obj, rem, err := n.Resolver.ResolveToLastNode(req.Context(), p)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&ResolveOutput{
			Cid:     obj.Cid(),
			RemPath: path.Join(rem),
		})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			output := res.Output().(*ResolveOutput)
			buf := new(bytes.Buffer)
			p := output.Cid.String()
			if output.RemPath != "" {
				p = path.Join([]string{p, output.RemPath})
			}

			buf.WriteString(p)

			return buf, nil
		},
	},
	Type: ResolveOutput{},
}
