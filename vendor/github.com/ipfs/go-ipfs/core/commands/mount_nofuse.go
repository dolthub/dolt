// +build linux darwin freebsd netbsd openbsd
// +build nofuse

package commands

import (
	cmds "github.com/ipfs/go-ipfs/commands"
)

var MountCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Mounts ipfs to the filesystem (disabled).",
		ShortDescription: `
This version of ipfs is compiled without fuse support, which is required
for mounting. If you'd like to be able to mount, please use a version of
ipfs compiled with fuse.

For the latest instructions, please check the project's repository:
  http://github.com/ipfs/go-ipfs
`,
	},
}
