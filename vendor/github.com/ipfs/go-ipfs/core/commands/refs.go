package commands

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	"github.com/ipfs/go-ipfs/core"
	dag "github.com/ipfs/go-ipfs/merkledag"
	path "github.com/ipfs/go-ipfs/path"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	node "gx/ipfs/QmPN7cwmpcc4DWXb4KTB9dNAJgjuPY69h3npsMfhRrQL9c/go-ipld-format"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

// KeyList is a general type for outputting lists of keys
type KeyList struct {
	Keys []*cid.Cid
}

// KeyListTextMarshaler outputs a KeyList as plaintext, one key per line
func KeyListTextMarshaler(res cmds.Response) (io.Reader, error) {
	output := res.Output().(*KeyList)
	buf := new(bytes.Buffer)
	for _, key := range output.Keys {
		buf.WriteString(key.String() + "\n")
	}
	return buf, nil
}

var RefsCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List links (references) from an object.",
		ShortDescription: `
Lists the hashes of all the links an IPFS or IPNS object(s) contains,
with the following format:

  <link base58 hash>

NOTE: List all references recursively by using the flag '-r'.
`,
	},
	Subcommands: map[string]*cmds.Command{
		"local": RefsLocalCmd,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("ipfs-path", true, true, "Path to the object(s) to list refs from.").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.StringOption("format", "Emit edges with given format. Available tokens: <src> <dst> <linkname>.").Default("<dst>"),
		cmds.BoolOption("edges", "e", "Emit edge format: `<from> -> <to>`.").Default(false),
		cmds.BoolOption("unique", "u", "Omit duplicate refs from output.").Default(false),
		cmds.BoolOption("recursive", "r", "Recursively list links of child nodes.").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		ctx := req.Context()
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		unique, _, err := req.Option("unique").Bool()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		recursive, _, err := req.Option("recursive").Bool()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		format, _, err := req.Option("format").String()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		edges, _, err := req.Option("edges").Bool()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if edges {
			if format != "<dst>" {
				res.SetError(errors.New("using format arguement with edges is not allowed"),
					cmds.ErrClient)
				return
			}

			format = "<src> -> <dst>"
		}

		objs, err := objectsForPaths(ctx, n, req.Arguments())
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		out := make(chan interface{})
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer close(out)

			rw := RefWriter{
				out:       out,
				DAG:       n.DAG,
				Ctx:       ctx,
				Unique:    unique,
				PrintFmt:  format,
				Recursive: recursive,
			}

			for _, o := range objs {
				if _, err := rw.WriteRefs(o); err != nil {
					out <- &RefWrapper{Err: err.Error()}
					return
				}
			}
		}()
	},
	Marshalers: refsMarshallerMap,
	Type:       RefWrapper{},
}

var RefsLocalCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "List all local references.",
		ShortDescription: `
Displays the hashes of all local objects.
`,
	},

	Run: func(req cmds.Request, res cmds.Response) {
		ctx := req.Context()
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		// todo: make async
		allKeys, err := n.Blockstore.AllKeysChan(ctx)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		out := make(chan interface{})
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer close(out)

			for k := range allKeys {
				out <- &RefWrapper{Ref: k.String()}
			}
		}()
	},
	Marshalers: refsMarshallerMap,
	Type:       RefWrapper{},
}

var refsMarshallerMap = cmds.MarshalerMap{
	cmds.Text: func(res cmds.Response) (io.Reader, error) {
		outChan, ok := res.Output().(<-chan interface{})
		if !ok {
			return nil, u.ErrCast()
		}

		marshal := func(v interface{}) (io.Reader, error) {
			obj, ok := v.(*RefWrapper)
			if !ok {
				return nil, u.ErrCast()
			}

			if obj.Err != "" {
				return nil, errors.New(obj.Err)
			}

			return strings.NewReader(obj.Ref + "\n"), nil
		}

		return &cmds.ChannelMarshaler{
			Channel:   outChan,
			Marshaler: marshal,
			Res:       res,
		}, nil
	},
}

func objectsForPaths(ctx context.Context, n *core.IpfsNode, paths []string) ([]node.Node, error) {
	objects := make([]node.Node, len(paths))
	for i, sp := range paths {
		p, err := path.ParsePath(sp)
		if err != nil {
			return nil, err
		}

		o, err := core.Resolve(ctx, n.Namesys, n.Resolver, p)
		if err != nil {
			return nil, err
		}
		objects[i] = o
	}
	return objects, nil
}

type RefWrapper struct {
	Ref string
	Err string
}

type RefWriter struct {
	out chan interface{}
	DAG dag.DAGService
	Ctx context.Context

	Unique    bool
	Recursive bool
	PrintFmt  string

	seen *cid.Set
}

// WriteRefs writes refs of the given object to the underlying writer.
func (rw *RefWriter) WriteRefs(n node.Node) (int, error) {
	if rw.Recursive {
		return rw.writeRefsRecursive(n)
	}
	return rw.writeRefsSingle(n)
}

func (rw *RefWriter) writeRefsRecursive(n node.Node) (int, error) {
	nc := n.Cid()

	var count int
	for i, ng := range dag.GetDAG(rw.Ctx, rw.DAG, n) {
		lc := n.Links()[i].Cid
		if rw.skip(lc) {
			continue
		}

		if err := rw.WriteEdge(nc, lc, n.Links()[i].Name); err != nil {
			return count, err
		}

		nd, err := ng.Get(rw.Ctx)
		if err != nil {
			return count, err
		}

		c, err := rw.writeRefsRecursive(nd)
		count += c
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

func (rw *RefWriter) writeRefsSingle(n node.Node) (int, error) {
	c := n.Cid()

	if rw.skip(c) {
		return 0, nil
	}

	count := 0
	for _, l := range n.Links() {
		lc := l.Cid
		if rw.skip(lc) {
			continue
		}

		if err := rw.WriteEdge(c, lc, l.Name); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// skip returns whether to skip a cid
func (rw *RefWriter) skip(c *cid.Cid) bool {
	if !rw.Unique {
		return false
	}

	if rw.seen == nil {
		rw.seen = cid.NewSet()
	}

	has := rw.seen.Has(c)
	if !has {
		rw.seen.Add(c)
	}
	return has
}

// Write one edge
func (rw *RefWriter) WriteEdge(from, to *cid.Cid, linkname string) error {
	if rw.Ctx != nil {
		select {
		case <-rw.Ctx.Done(): // just in case.
			return rw.Ctx.Err()
		default:
		}
	}

	var s string
	switch {
	case rw.PrintFmt != "":
		s = rw.PrintFmt
		s = strings.Replace(s, "<src>", from.String(), -1)
		s = strings.Replace(s, "<dst>", to.String(), -1)
		s = strings.Replace(s, "<linkname>", linkname, -1)
	default:
		s += to.String()
	}

	rw.out <- &RefWrapper{Ref: s}
	return nil
}
