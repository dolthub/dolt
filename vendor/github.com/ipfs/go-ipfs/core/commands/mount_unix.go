// +build linux darwin freebsd netbsd openbsd
// +build !nofuse

package commands

import (
	"fmt"
	"io"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	nodeMount "github.com/ipfs/go-ipfs/fuse/node"
	config "github.com/ipfs/go-ipfs/repo/config"
)

var MountCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Mounts IPFS to the filesystem (read-only).",
		ShortDescription: `
Mount IPFS at a read-only mountpoint on the OS (default: /ipfs and /ipns).
All IPFS objects will be accessible under that directory. Note that the
root will not be listable, as it is virtual. Access known paths directly.

You may have to create /ipfs and /ipns before using 'ipfs mount':

> sudo mkdir /ipfs /ipns
> sudo chown ` + "`" + `whoami` + "`" + ` /ipfs /ipns
> ipfs daemon &
> ipfs mount
`,
		LongDescription: `
Mount IPFS at a read-only mountpoint on the OS. The default, /ipfs and /ipns,
are set in the configutation file, but can be overriden by the options.
All IPFS objects will be accessible under this directory. Note that the
root will not be listable, as it is virtual. Access known paths directly.

You may have to create /ipfs and /ipns before using 'ipfs mount':

> sudo mkdir /ipfs /ipns
> sudo chown ` + "`" + `whoami` + "`" + ` /ipfs /ipns
> ipfs daemon &
> ipfs mount

Example:

# setup
> mkdir foo
> echo "baz" > foo/bar
> ipfs add -r foo
added QmWLdkp93sNxGRjnFHPaYg8tCQ35NBY3XPn6KiETd3Z4WR foo/bar
added QmSh5e7S6fdcu75LAbXNZAFY2nGyZUJXyLCJDvn2zRkWyC foo
> ipfs ls QmSh5e7S6fdcu75LAbXNZAFY2nGyZUJXyLCJDvn2zRkWyC
QmWLdkp93sNxGRjnFHPaYg8tCQ35NBY3XPn6KiETd3Z4WR 12 bar
> ipfs cat QmWLdkp93sNxGRjnFHPaYg8tCQ35NBY3XPn6KiETd3Z4WR
baz

# mount
> ipfs daemon &
> ipfs mount
IPFS mounted at: /ipfs
IPNS mounted at: /ipns
> cd /ipfs/QmSh5e7S6fdcu75LAbXNZAFY2nGyZUJXyLCJDvn2zRkWyC
> ls
bar
> cat bar
baz
> cat /ipfs/QmSh5e7S6fdcu75LAbXNZAFY2nGyZUJXyLCJDvn2zRkWyC/bar
baz
> cat /ipfs/QmWLdkp93sNxGRjnFHPaYg8tCQ35NBY3XPn6KiETd3Z4WR
baz
`,
	},
	Options: []cmds.Option{
		cmds.StringOption("ipfs-path", "f", "The path where IPFS should be mounted."),
		cmds.StringOption("ipns-path", "n", "The path where IPNS should be mounted."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		cfg, err := req.InvocContext().GetConfig()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		node, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		// error if we aren't running node in online mode
		if !node.OnlineMode() {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		fsdir, found, err := req.Option("f").String()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if !found {
			fsdir = cfg.Mounts.IPFS // use default value
		}

		// get default mount points
		nsdir, found, err := req.Option("n").String()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		if !found {
			nsdir = cfg.Mounts.IPNS // NB: be sure to not redeclare!
		}

		err = nodeMount.Mount(node, fsdir, nsdir)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		var output config.Mounts
		output.IPFS = fsdir
		output.IPNS = nsdir
		res.SetOutput(&output)
	},
	Type: config.Mounts{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			v := res.Output().(*config.Mounts)
			s := fmt.Sprintf("IPFS mounted at: %s\n", v.IPFS)
			s += fmt.Sprintf("IPNS mounted at: %s\n", v.IPNS)
			return strings.NewReader(s), nil
		},
	},
}
