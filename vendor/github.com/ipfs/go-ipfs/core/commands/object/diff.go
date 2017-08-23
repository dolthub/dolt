package objectcmd

import (
	"bytes"
	"fmt"
	"io"

	cmds "github.com/ipfs/go-ipfs/commands"
	core "github.com/ipfs/go-ipfs/core"
	dagutils "github.com/ipfs/go-ipfs/merkledag/utils"
	path "github.com/ipfs/go-ipfs/path"
)

type Changes struct {
	Changes []*dagutils.Change
}

var ObjectDiffCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Display the diff between two ipfs objects.",
		ShortDescription: `
'ipfs object diff' is a command used to show the differences between
two IPFS objects.`,
		LongDescription: `
'ipfs object diff' is a command used to show the differences between
two IPFS objects.

Example:

   > ls foo
   bar baz/ giraffe
   > ipfs add -r foo
   ...
   Added QmegHcnrPgMwC7tBiMxChD54fgQMBUecNw9nE9UUU4x1bz foo
   > OBJ_A=QmegHcnrPgMwC7tBiMxChD54fgQMBUecNw9nE9UUU4x1bz
   > echo "different content" > foo/bar
   > ipfs add -r foo
   ...
   Added QmcmRptkSPWhptCttgHg27QNDmnV33wAJyUkCnAvqD3eCD foo
   > OBJ_B=QmcmRptkSPWhptCttgHg27QNDmnV33wAJyUkCnAvqD3eCD
   > ipfs object diff -v $OBJ_A $OBJ_B
   Changed "bar" from QmNgd5cz2jNftnAHBhcRUGdtiaMzb5Rhjqd4etondHHST8 to QmRfFVsjSXkhFxrfWnLpMae2M4GBVsry6VAuYYcji5MiZb.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("obj_a", true, false, "Object to diff against."),
		cmds.StringArg("obj_b", true, false, "Object to diff."),
	},
	Options: []cmds.Option{
		cmds.BoolOption("verbose", "v", "Print extra information."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		node, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		a := req.Arguments()[0]
		b := req.Arguments()[1]

		pa, err := path.ParsePath(a)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		pb, err := path.ParsePath(b)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		ctx := req.Context()

		obj_a, err := core.Resolve(ctx, node.Namesys, node.Resolver, pa)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		obj_b, err := core.Resolve(ctx, node.Namesys, node.Resolver, pb)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		changes, err := dagutils.Diff(ctx, node.DAG, obj_a, obj_b)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&Changes{changes})
	},
	Type: Changes{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			verbose, _, _ := res.Request().Option("v").Bool()
			changes := res.Output().(*Changes)
			buf := new(bytes.Buffer)
			for _, change := range changes.Changes {
				if verbose {
					switch change.Type {
					case dagutils.Add:
						fmt.Fprintf(buf, "Added new link %q pointing to %s.\n", change.Path, change.After)
					case dagutils.Mod:
						fmt.Fprintf(buf, "Changed %q from %s to %s.\n", change.Path, change.Before, change.After)
					case dagutils.Remove:
						fmt.Fprintf(buf, "Removed link %q (was %s).\n", change.Path, change.Before)
					}
				} else {
					switch change.Type {
					case dagutils.Add:
						fmt.Fprintf(buf, "+ %s %q\n", change.After, change.Path)
					case dagutils.Mod:
						fmt.Fprintf(buf, "~ %s %s %q\n", change.Before, change.After, change.Path)
					case dagutils.Remove:
						fmt.Fprintf(buf, "- %s %q\n", change.Before, change.Path)
					}
				}
			}
			return buf, nil
		},
	},
}
