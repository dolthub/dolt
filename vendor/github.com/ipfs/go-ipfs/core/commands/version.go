package commands

import (
	"fmt"
	"io"
	"runtime"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
	config "github.com/ipfs/go-ipfs/repo/config"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
)

type VersionOutput struct {
	Version string
	Commit  string
	Repo    string
	System  string
	Golang  string
}

var VersionCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline:          "Show ipfs version information.",
		ShortDescription: "Returns the current version of ipfs and exits.",
	},

	Options: []cmds.Option{
		cmds.BoolOption("number", "n", "Only show the version number.").Default(false),
		cmds.BoolOption("commit", "Show the commit hash.").Default(false),
		cmds.BoolOption("repo", "Show repo version.").Default(false),
		cmds.BoolOption("all", "Show all version information").Default(false),
	},
	Run: func(req cmds.Request, res cmds.Response) {
		res.SetOutput(&VersionOutput{
			Version: config.CurrentVersionNumber,
			Commit:  config.CurrentCommit,
			Repo:    fmt.Sprint(fsrepo.RepoVersion),
			System:  runtime.GOARCH + "/" + runtime.GOOS, //TODO: Precise version here
			Golang:  runtime.Version(),
		})
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			v := res.Output().(*VersionOutput)

			repo, _, err := res.Request().Option("repo").Bool()
			if err != nil {
				return nil, err
			}

			if repo {
				return strings.NewReader(v.Repo + "\n"), nil
			}

			commit, _, err := res.Request().Option("commit").Bool()
			commitTxt := ""
			if err != nil {
				return nil, err
			}
			if commit {
				commitTxt = "-" + v.Commit
			}

			number, _, err := res.Request().Option("number").Bool()
			if err != nil {
				return nil, err
			}
			if number {
				return strings.NewReader(fmt.Sprintln(v.Version + commitTxt)), nil
			}

			all, _, err := res.Request().Option("all").Bool()
			if err != nil {
				return nil, err
			}
			if all {
				out := fmt.Sprintf("go-ipfs version: %s-%s\n"+
					"Repo version: %s\nSystem version: %s\nGolang version: %s\n",
					v.Version, v.Commit, v.Repo, v.System, v.Golang)
				return strings.NewReader(out), nil
			}

			return strings.NewReader(fmt.Sprintf("ipfs version %s%s\n", v.Version, commitTxt)), nil
		},
	},
	Type: VersionOutput{},
}
