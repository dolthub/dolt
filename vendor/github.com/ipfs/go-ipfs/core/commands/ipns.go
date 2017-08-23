package commands

import (
	"errors"
	"io"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	namesys "github.com/ipfs/go-ipfs/namesys"
	offline "github.com/ipfs/go-ipfs/routing/offline"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

var IpnsCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Resolve IPNS names.",
		ShortDescription: `
IPNS is a PKI namespace, where names are the hashes of public keys, and
the private key enables publishing new (signed) values. In both publish
and resolve, the default name used is the node's own PeerID,
which is the hash of its public key.
`,
		LongDescription: `
IPNS is a PKI namespace, where names are the hashes of public keys, and
the private key enables publishing new (signed) values. In both publish
and resolve, the default name used is the node's own PeerID,
which is the hash of its public key.

You can use the 'ipfs key' commands to list and generate more names and their respective keys.

Examples:

Resolve the value of your name:

  > ipfs name resolve
  /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy

Resolve the value of another name:

  > ipfs name resolve QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ
  /ipfs/QmSiTko9JZyabH56y2fussEt1A5oDqsFXB3CkvAqraFryz

Resolve the value of a dnslink:

  > ipfs name resolve ipfs.io
  /ipfs/QmaBvfZooxWkrv7D3r8LS9moNjzD2o525XMZze69hhoxf5

`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("name", false, false, "The IPNS name to resolve. Defaults to your node's peerID."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("recursive", "r", "Resolve until the result is not an IPNS name.").Default(false),
		cmds.BoolOption("nocache", "n", "Do not use cached entries.").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {

		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if !n.OnlineMode() {
			err := n.SetupOfflineRouting()
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}
		}

		nocache, _, _ := req.Option("nocache").Bool()
		local, _, _ := req.Option("local").Bool()

		// default to nodes namesys resolver
		var resolver namesys.Resolver = n.Namesys

		if local && nocache {
			res.SetError(errors.New("cannot specify both local and nocache"), cmds.ErrNormal)
			return
		}

		if local {
			offroute := offline.NewOfflineRouter(n.Repo.Datastore(), n.PrivateKey)
			resolver = namesys.NewRoutingResolver(offroute, 0)
		}

		if nocache {
			resolver = namesys.NewNameSystem(n.Routing, n.Repo.Datastore(), 0)
		}

		var name string
		if len(req.Arguments()) == 0 {
			if n.Identity == "" {
				res.SetError(errors.New("identity not loaded"), cmds.ErrNormal)
				return
			}
			name = n.Identity.Pretty()

		} else {
			name = req.Arguments()[0]
		}

		recursive, _, _ := req.Option("recursive").Bool()
		depth := 1
		if recursive {
			depth = namesys.DefaultDepthLimit
		}

		if !strings.HasPrefix(name, "/ipns/") {
			name = "/ipns/" + name
		}

		output, err := resolver.ResolveN(req.Context(), name, depth)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		// TODO: better errors (in the case of not finding the name, we get "failed to find any peer in table")

		res.SetOutput(&ResolvedPath{output})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			output, ok := res.Output().(*ResolvedPath)
			if !ok {
				return nil, u.ErrCast()
			}
			return strings.NewReader(output.Path.String() + "\n"), nil
		},
	},
	Type: ResolvedPath{},
}
