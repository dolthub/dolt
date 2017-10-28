package commands

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	cmds "github.com/ipfs/go-ipfs/commands"
	corerepo "github.com/ipfs/go-ipfs/core/corerepo"
	config "github.com/ipfs/go-ipfs/repo/config"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	lockfile "github.com/ipfs/go-ipfs/repo/fsrepo/lock"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	u "gx/ipfs/QmSU6eubNdhXjFBJBSksTp8kv8YRub8mGAPv8tVJHmL2EU/go-ipfs-util"
)

type RepoVersion struct {
	Version string
}

var RepoCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Manipulate the IPFS repo.",
		ShortDescription: `
'ipfs repo' is a plumbing command used to manipulate the repo.
`,
	},

	Subcommands: map[string]*cmds.Command{
		"gc":      repoGcCmd,
		"stat":    repoStatCmd,
		"fsck":    RepoFsckCmd,
		"version": repoVersionCmd,
		"verify":  repoVerifyCmd,
	},
}

// GcResult is the result returned by "repo gc" command.
type GcResult struct {
	Key   *cid.Cid
	Error string `json:",omitempty"`
}

var repoGcCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Perform a garbage collection sweep on the repo.",
		ShortDescription: `
'ipfs repo gc' is a plumbing command that will sweep the local
set of stored objects and remove ones that are not pinned in
order to reclaim hard disk space.
`,
	},
	Options: []cmds.Option{
		cmds.BoolOption("quiet", "q", "Write minimal output.").Default(false),
		cmds.BoolOption("stream-errors", "Stream errors.").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		streamErrors, _, _ := res.Request().Option("stream-errors").Bool()

		gcOutChan := corerepo.GarbageCollectAsync(n, req.Context())

		outChan := make(chan interface{}, cap(gcOutChan))
		res.SetOutput((<-chan interface{})(outChan))

		go func() {
			defer close(outChan)
			if streamErrors {
				errs := false
				for res := range gcOutChan {
					if res.Error != nil {
						outChan <- &GcResult{Error: res.Error.Error()}
						errs = true
					} else {
						outChan <- &GcResult{Key: res.KeyRemoved}
					}
				}
				if errs {
					res.SetError(fmt.Errorf("encountered errors during gc run"), cmds.ErrNormal)
				}
			} else {
				err := corerepo.CollectResult(req.Context(), gcOutChan, func(k *cid.Cid) {
					outChan <- &GcResult{Key: k}
				})
				if err != nil {
					res.SetError(err, cmds.ErrNormal)
				}
			}
		}()
	},
	Type: GcResult{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			outChan, ok := res.Output().(<-chan interface{})
			if !ok {
				return nil, u.ErrCast()
			}

			quiet, _, err := res.Request().Option("quiet").Bool()
			if err != nil {
				return nil, err
			}

			marshal := func(v interface{}) (io.Reader, error) {
				obj, ok := v.(*GcResult)
				if !ok {
					return nil, u.ErrCast()
				}

				if obj.Error != "" {
					fmt.Fprintf(res.Stderr(), "Error: %s\n", obj.Error)
					return nil, nil
				}

				if quiet {
					return bytes.NewBufferString(obj.Key.String() + "\n"), nil
				} else {
					return bytes.NewBufferString(fmt.Sprintf("removed %s\n", obj.Key)), nil
				}
			}

			return &cmds.ChannelMarshaler{
				Channel:   outChan,
				Marshaler: marshal,
				Res:       res,
			}, nil
		},
	},
}

var repoStatCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Get stats for the currently used repo.",
		ShortDescription: `
'ipfs repo stat' is a plumbing command that will scan the local
set of stored objects and print repo statistics. It outputs to stdout:
NumObjects      int Number of objects in the local repo.
RepoPath        string The path to the repo being currently used.
RepoSize        int Size in bytes that the repo is currently taking.
Version         string The repo version.
`,
	},
	Run: func(req cmds.Request, res cmds.Response) {
		n, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		stat, err := corerepo.RepoStat(n, req.Context())
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(stat)
	},
	Options: []cmds.Option{
		cmds.BoolOption("human", "Output RepoSize in MiB.").Default(false),
	},
	Type: corerepo.Stat{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			stat, ok := res.Output().(*corerepo.Stat)
			if !ok {
				return nil, u.ErrCast()
			}

			human, _, err := res.Request().Option("human").Bool()
			if err != nil {
				return nil, err
			}

			buf := new(bytes.Buffer)
			wtr := tabwriter.NewWriter(buf, 0, 0, 1, ' ', 0)
			fmt.Fprintf(wtr, "NumObjects:\t%d\n", stat.NumObjects)
			sizeInMiB := stat.RepoSize / (1024 * 1024)
			if human && sizeInMiB > 0 {
				fmt.Fprintf(wtr, "RepoSize (MiB):\t%d\n", sizeInMiB)
			} else {
				fmt.Fprintf(wtr, "RepoSize:\t%d\n", stat.RepoSize)
			}
			if stat.StorageMax != corerepo.NoLimit {
				maxSizeInMiB := stat.StorageMax / (1024 * 1024)
				if human && maxSizeInMiB > 0 {
					fmt.Fprintf(wtr, "StorageMax (MiB):\t%d\n", maxSizeInMiB)
				} else {
					fmt.Fprintf(wtr, "StorageMax:\t%d\n", stat.StorageMax)
				}
			}
			fmt.Fprintf(wtr, "RepoPath:\t%s\n", stat.RepoPath)
			fmt.Fprintf(wtr, "Version:\t%s\n", stat.Version)
			wtr.Flush()

			return buf, nil
		},
	},
}

var RepoFsckCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Remove repo lockfiles.",
		ShortDescription: `
'ipfs repo fsck' is a plumbing command that will remove repo and level db
lockfiles, as well as the api file. This command can only run when no ipfs
daemons are running.
`,
	},
	Run: func(req cmds.Request, res cmds.Response) {
		configRoot := req.InvocContext().ConfigRoot

		dsPath, err := config.DataStorePath(configRoot)
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		dsLockFile := filepath.Join(dsPath, "LOCK") // TODO: get this lockfile programmatically
		repoLockFile := filepath.Join(configRoot, lockfile.LockFile)
		apiFile := filepath.Join(configRoot, "api") // TODO: get this programmatically

		log.Infof("Removing repo lockfile: %s", repoLockFile)
		log.Infof("Removing datastore lockfile: %s", dsLockFile)
		log.Infof("Removing api file: %s", apiFile)

		err = os.Remove(repoLockFile)
		if err != nil && !os.IsNotExist(err) {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		err = os.Remove(dsLockFile)
		if err != nil && !os.IsNotExist(err) {
			res.SetError(err, cmds.ErrNormal)
			return
		}
		err = os.Remove(apiFile)
		if err != nil && !os.IsNotExist(err) {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		res.SetOutput(&MessageOutput{"Lockfiles have been removed.\n"})
	},
	Type: MessageOutput{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: MessageTextMarshaler,
	},
}

type VerifyProgress struct {
	Message  string
	Progress int
}

var repoVerifyCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Verify all blocks in repo are not corrupted.",
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		out := make(chan interface{})
		go func() {
			defer close(out)
			bs := bstore.NewBlockstore(nd.Repo.Datastore())

			bs.HashOnRead(true)

			keys, err := bs.AllKeysChan(req.Context())
			if err != nil {
				log.Error(err)
				return
			}

			var fails int
			var i int
			for k := range keys {
				_, err := bs.Get(k)
				if err != nil {
					out <- &VerifyProgress{
						Message: fmt.Sprintf("block %s was corrupt (%s)", k, err),
					}
					fails++
				}
				i++
				out <- &VerifyProgress{Progress: i}
			}
			if fails == 0 {
				out <- &VerifyProgress{Message: "verify complete, all blocks validated."}
			} else {
				out <- &VerifyProgress{Message: "verify complete, some blocks were corrupt."}
			}
		}()

		res.SetOutput((<-chan interface{})(out))
	},
	Type: VerifyProgress{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			out := res.Output().(<-chan interface{})

			marshal := func(v interface{}) (io.Reader, error) {
				obj, ok := v.(*VerifyProgress)
				if !ok {
					return nil, u.ErrCast()
				}

				buf := new(bytes.Buffer)
				if obj.Message != "" {
					if strings.Contains(obj.Message, "blocks were corrupt") {
						return nil, fmt.Errorf(obj.Message)
					}
					if len(obj.Message) < 20 {
						obj.Message += "             "
					}
					fmt.Fprintln(buf, obj.Message)
					return buf, nil
				}

				fmt.Fprintf(buf, "%d blocks processed.\r", obj.Progress)
				return buf, nil
			}

			return &cmds.ChannelMarshaler{
				Channel:   out,
				Marshaler: marshal,
				Res:       res,
			}, nil
		},
	},
}

var repoVersionCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Show the repo version.",
		ShortDescription: `
'ipfs repo version' returns the current repo version.
`,
	},

	Options: []cmds.Option{
		cmds.BoolOption("quiet", "q", "Write minimal output."),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		res.SetOutput(&RepoVersion{
			Version: fmt.Sprint(fsrepo.RepoVersion),
		})
	},
	Type: RepoVersion{},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			response := res.Output().(*RepoVersion)

			quiet, _, err := res.Request().Option("quiet").Bool()
			if err != nil {
				return nil, err
			}

			buf := new(bytes.Buffer)
			if quiet {
				buf = bytes.NewBufferString(fmt.Sprintf("fs-repo@%s\n", response.Version))
			} else {
				buf = bytes.NewBufferString(fmt.Sprintf("ipfs repo version fs-repo@%s\n", response.Version))
			}
			return buf, nil

		},
	},
}
