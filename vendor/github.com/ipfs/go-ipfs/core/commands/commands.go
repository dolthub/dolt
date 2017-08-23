// Package commands implements the ipfs command interface
//
// Using github.com/ipfs/go-ipfs/commands to define the command line and HTTP
// APIs.  This is the interface available to folks using IPFS from outside of
// the Go language.
package commands

import (
	"bytes"
	"io"
	"sort"
	"strings"

	cmds "github.com/ipfs/go-ipfs/commands"
)

type Command struct {
	Name        string
	Subcommands []Command
	Options     []Option
}

type Option struct {
	Names []string
}

const (
	flagsOptionName = "flags"
)

// CommandsCmd takes in a root command,
// and returns a command that lists the subcommands in that root
func CommandsCmd(root *cmds.Command) *cmds.Command {
	return &cmds.Command{
		Helptext: cmds.HelpText{
			Tagline:          "List all available commands.",
			ShortDescription: `Lists all available commands (and subcommands) and exits.`,
		},
		Options: []cmds.Option{
			cmds.BoolOption(flagsOptionName, "f", "Show command flags").Default(false),
		},
		Run: func(req cmds.Request, res cmds.Response) {
			rootCmd := cmd2outputCmd("ipfs", root)
			res.SetOutput(&rootCmd)
		},
		Marshalers: cmds.MarshalerMap{
			cmds.Text: func(res cmds.Response) (io.Reader, error) {
				v := res.Output().(*Command)
				showOptions, _, _ := res.Request().Option(flagsOptionName).Bool()
				buf := new(bytes.Buffer)
				for _, s := range cmdPathStrings(v, showOptions) {
					buf.Write([]byte(s + "\n"))
				}
				return buf, nil
			},
		},
		Type: Command{},
	}
}

func cmd2outputCmd(name string, cmd *cmds.Command) Command {
	opts := make([]Option, len(cmd.Options))
	for i, opt := range cmd.Options {
		opts[i] = Option{opt.Names()}
	}

	output := Command{
		Name:        name,
		Subcommands: make([]Command, len(cmd.Subcommands)),
		Options:     opts,
	}

	i := 0
	for name, sub := range cmd.Subcommands {
		output.Subcommands[i] = cmd2outputCmd(name, sub)
		i++
	}

	return output
}

func cmdPathStrings(cmd *Command, showOptions bool) []string {
	var cmds []string

	var recurse func(prefix string, cmd *Command)
	recurse = func(prefix string, cmd *Command) {
		newPrefix := prefix + cmd.Name
		cmds = append(cmds, newPrefix)
		if prefix != "" && showOptions {
			for _, options := range cmd.Options {
				var cmdOpts []string
				for _, flag := range options.Names {
					if len(flag) == 1 {
						flag = "-" + flag
					} else {
						flag = "--" + flag
					}
					cmdOpts = append(cmdOpts, newPrefix+" "+flag)
				}
				cmds = append(cmds, strings.Join(cmdOpts, " / "))
			}
		}
		for _, sub := range cmd.Subcommands {
			recurse(newPrefix+" ", &sub)
		}
	}

	recurse("", cmd)
	sort.Sort(sort.StringSlice(cmds))
	return cmds
}
