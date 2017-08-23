package objectcmd

import (
	"io"
	"io/ioutil"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	dag "github.com/ipfs/go-ipfs/merkledag"
	dagutils "github.com/ipfs/go-ipfs/merkledag/utils"
	path "github.com/ipfs/go-ipfs/path"
	ft "github.com/ipfs/go-ipfs/unixfs"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

var ObjectPatchCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Create a new merkledag object based on an existing one.",
		ShortDescription: `
'ipfs object patch <root> <cmd> <args>' is a plumbing command used to
build custom DAG objects. It mutates objects, creating new objects as a
result. This is the Merkle-DAG version of modifying an object.
`,
	},
	Arguments: []cmds.Argument{},
	Subcommands: map[string]*cmds.Command{
		"append-data": patchAppendDataCmd,
		"add-link":    patchAddLinkCmd,
		"rm-link":     patchRmLinkCmd,
		"set-data":    patchSetDataCmd,
	},
}

func objectMarshaler(res cmds.Response) (io.Reader, error) {
	o, ok := res.Output().(*Object)
	if !ok {
		return nil, u.ErrCast()
	}

	return strings.NewReader(o.Hash + "\n"), nil
}

var patchAppendDataCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Append data to the data segment of a dag node.",
		ShortDescription: `
Append data to what already exists in the data segment in the given object.

Example:

	$ echo "hello" | ipfs object patch $HASH append-data

NOTE: This does not append data to a file - it modifies the actual raw
data within an object. Objects have a max size of 1MB and objects larger than
the limit will not be respected by the network.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("root", true, false, "The hash of the node to modify."),
		cmds.FileArg("data", true, false, "Data to append.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		root, err := path.ParsePath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rootnd, err := core.Resolve(req.Context(), nd.Namesys, nd.Resolver, root)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rtpb, ok := rootnd.(*dag.ProtoNode)
		if !ok {
			res.SetError(dag.ErrNotProtobuf, cmds.ErrNormal)
			return
		}

		fi, err := req.Files().NextFile()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		data, err := ioutil.ReadAll(fi)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rtpb.SetData(append(rtpb.Data(), data...))

		newkey, err := nd.DAG.Add(rtpb)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&Object{Hash: newkey.String()})
	},
	Type: Object{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: objectMarshaler,
	},
}

var patchSetDataCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Set the data field of an IPFS object.",
		ShortDescription: `
Set the data of an IPFS object from stdin or with the contents of a file.

Example:

    $ echo "my data" | ipfs object patch $MYHASH set-data
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("root", true, false, "The hash of the node to modify."),
		cmds.FileArg("data", true, false, "The data to set the object to.").EnableStdin(),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rp, err := path.ParsePath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		root, err := core.Resolve(req.Context(), nd.Namesys, nd.Resolver, rp)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rtpb, ok := root.(*dag.ProtoNode)
		if !ok {
			res.SetError(dag.ErrNotProtobuf, cmds.ErrNormal)
			return
		}

		fi, err := req.Files().NextFile()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		data, err := ioutil.ReadAll(fi)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rtpb.SetData(data)

		newkey, err := nd.DAG.Add(rtpb)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&Object{Hash: newkey.String()})
	},
	Type: Object{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: objectMarshaler,
	},
}

var patchRmLinkCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Remove a link from an object.",
		ShortDescription: `
Removes a link by the given name from root.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("root", true, false, "The hash of the node to modify."),
		cmds.StringArg("link", true, false, "Name of the link to remove."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rootp, err := path.ParsePath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		root, err := core.Resolve(req.Context(), nd.Namesys, nd.Resolver, rootp)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rtpb, ok := root.(*dag.ProtoNode)
		if !ok {
			res.SetError(dag.ErrNotProtobuf, cmds.ErrNormal)
			return
		}

		path := req.Arguments()[1]

		e := dagutils.NewDagEditor(rtpb, nd.DAG)

		err = e.RmLink(req.Context(), path)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		nnode, err := e.Finalize(nd.DAG)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		nc := nnode.Cid()

		res.SetOutput(&Object{Hash: nc.String()})
	},
	Type: Object{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: objectMarshaler,
	},
}

var patchAddLinkCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Add a link to a given object.",
		ShortDescription: `
Add a Merkle-link to the given object and return the hash of the result.

Example:

    $ EMPTY_DIR=$(ipfs object new unixfs-dir)
    $ BAR=$(echo "bar" | ipfs add -q)
    $ ipfs object patch $EMPTY_DIR add-link foo $BAR

This takes an empty directory, and adds a link named 'foo' under it, pointing
to a file containing 'bar', and returns the hash of the new object.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("root", true, false, "The hash of the node to modify."),
		cmds.StringArg("name", true, false, "Name of link to create."),
		cmds.StringArg("ref", true, false, "IPFS object to add link to."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("create", "p", "Create intermediary nodes.").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rootp, err := path.ParsePath(req.Arguments()[0])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		root, err := core.Resolve(req.Context(), nd.Namesys, nd.Resolver, rootp)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		rtpb, ok := root.(*dag.ProtoNode)
		if !ok {
			res.SetError(dag.ErrNotProtobuf, cmds.ErrNormal)
			return
		}

		npath := req.Arguments()[1]
		childp, err := path.ParsePath(req.Arguments()[2])
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		create, _, err := req.Option("create").Bool()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		var createfunc func() *dag.ProtoNode
		if create {
			createfunc = ft.EmptyDirNode
		}

		e := dagutils.NewDagEditor(rtpb, nd.DAG)

		childnd, err := core.Resolve(req.Context(), nd.Namesys, nd.Resolver, childp)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		chpb, ok := childnd.(*dag.ProtoNode)
		if !ok {
			res.SetError(dag.ErrNotProtobuf, cmds.ErrNormal)
			return
		}

		err = e.InsertNodeAtPath(req.Context(), npath, chpb, createfunc)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		nnode, err := e.Finalize(nd.DAG)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		nc := nnode.Cid()

		res.SetOutput(&Object{Hash: nc.String()})
	},
	Type: Object{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: objectMarshaler,
	},
}
