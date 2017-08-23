package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cmds "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	keystore "github.com/ipfs/go-ipfs/keystore"
	path "github.com/ipfs/go-ipfs/path"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	crypto "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"
)

var errNotOnline = errors.New("This command must be run in online mode. Try running 'ipfs daemon' first.")

var PublishCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Publish IPNS names.",
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

Publish an <ipfs-path> with your default name:

  > ipfs name publish /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
  Published to QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n: /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy

Publish an <ipfs-path> with another name, added by an 'ipfs key' command:

  > ipfs key gen --type=rsa --size=2048 mykey
  > ipfs name publish --key=mykey /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
  Published to QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n: /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy

Alternatively, publish an <ipfs-path> using a valid PeerID(as listed by 'ipfs key list -l'):

 > ipfs name publish --key=QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy
  Published to QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n: /ipfs/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy

`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("ipfs-path", true, false, "ipfs path of the object to be published.").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.BoolOption("resolve", "Resolve given path before publishing.").Default(true),
		cmds.StringOption("lifetime", "t",
			`Time duration that the record will be valid for. <<default>>
    This accepts durations such as "300s", "1.5h" or "2h45m". Valid time units are
    "ns", "us" (or "µs"), "ms", "s", "m", "h".`).Default("24h"),
		cmds.StringOption("ttl", "Time duration this record should be cached for (caution: experimental)."),
		cmds.StringOption("key", "k", "Name of the key to be used or a valid PeerID, as listed by 'ipfs key list -l'. Default: <<default>>.").Default("self"),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		log.Debug("begin publish")
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

		if n.Mounts.Ipns != nil && n.Mounts.Ipns.IsActive() {
			res.SetError(errors.New("cannot manually publish while IPNS is mounted"), cmds.ErrNormal)
			return
		}

		pstr := req.Arguments()[0]

		if n.Identity == "" {
			res.SetError(errors.New("identity not loaded"), cmds.ErrNormal)
			return
		}

		popts := new(publishOpts)

		popts.verifyExists, _, _ = req.Option("resolve").Bool()

		validtime, _, _ := req.Option("lifetime").String()
		d, err := time.ParseDuration(validtime)
		if err != nil {
			res.SetError(fmt.Errorf("error parsing lifetime option: %s", err), cmds.ErrNormal)
			return
		}

		popts.pubValidTime = d

		ctx := req.Context()
		if ttl, found, _ := req.Option("ttl").String(); found {
			d, err := time.ParseDuration(ttl)
			if err != nil {
				res.SetError(err, cmds.ErrNormal)
				return
			}

			ctx = context.WithValue(ctx, "ipns-publish-ttl", d)
		}

		kname, _, _ := req.Option("key").String()
		k, err := keylookup(n, kname)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		pth, err := path.ParsePath(pstr)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		output, err := publish(ctx, n, k, pth, popts)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		res.SetOutput(output)
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			v := res.Output().(*IpnsEntry)
			s := fmt.Sprintf("Published to %s: %s\n", v.Name, v.Value)
			return strings.NewReader(s), nil
		},
	},
	Type: IpnsEntry{},
}

type publishOpts struct {
	verifyExists bool
	pubValidTime time.Duration
}

func publish(ctx context.Context, n *core.IpfsNode, k crypto.PrivKey, ref path.Path, opts *publishOpts) (*IpnsEntry, error) {

	if opts.verifyExists {
		// verify the path exists
		_, err := core.Resolve(ctx, n.Namesys, n.Resolver, ref)
		if err != nil {
			return nil, err
		}
	}

	eol := time.Now().Add(opts.pubValidTime)
	err := n.Namesys.PublishWithEOL(ctx, k, ref, eol)
	if err != nil {
		return nil, err
	}

	pid, err := peer.IDFromPrivateKey(k)
	if err != nil {
		return nil, err
	}

	return &IpnsEntry{
		Name:  pid.Pretty(),
		Value: ref.String(),
	}, nil
}

func keylookup(n *core.IpfsNode, k string) (crypto.PrivKey, error) {

	res, err := n.GetKey(k)
	if res != nil {
		return res, nil
	}

	if err != nil && err != keystore.ErrNoSuchKey {
		return nil, err
	}

	keys, err := n.Repo.Keystore().List()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		privKey, err := n.Repo.Keystore().Get(key)
		if err != nil {
			return nil, err
		}

		pubKey := privKey.GetPublic()

		pid, err := peer.IDFromPublicKey(pubKey)
		if err != nil {
			return nil, err
		}

		if pid.Pretty() == k {
			return privKey, nil
		}
	}

	return nil, fmt.Errorf("no key by the given name or PeerID was found")
}
