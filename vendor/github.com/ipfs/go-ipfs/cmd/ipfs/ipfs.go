package main

import (
	"fmt"

	cmds "github.com/ipfs/go-ipfs/commands"
	commands "github.com/ipfs/go-ipfs/core/commands"
)

// This is the CLI root, used for executing commands accessible to CLI clients.
// Some subcommands (like 'ipfs daemon' or 'ipfs init') are only accessible here,
// and can't be called through the HTTP API.
var Root = &cmds.Command{
	Options:  commands.Root.Options,
	Helptext: commands.Root.Helptext,
}

// commandsClientCmd is the "ipfs commands" command for local cli
var commandsClientCmd = commands.CommandsCmd(Root)

// Commands in localCommands should always be run locally (even if daemon is running).
// They can override subcommands in commands.Root by defining a subcommand with the same name.
var localCommands = map[string]*cmds.Command{
	"daemon":   daemonCmd,
	"init":     initCmd,
	"commands": commandsClientCmd,
}
var localMap = make(map[*cmds.Command]bool)

func init() {
	// setting here instead of in literal to prevent initialization loop
	// (some commands make references to Root)
	Root.Subcommands = localCommands

	// copy all subcommands from commands.Root into this root (if they aren't already present)
	for k, v := range commands.Root.Subcommands {
		if _, found := Root.Subcommands[k]; !found {
			Root.Subcommands[k] = v
		}
	}

	for _, v := range localCommands {
		localMap[v] = true
	}
}

// NB: when necessary, properties are described using negatives in order to
// provide desirable defaults
type cmdDetails struct {
	cannotRunOnClient bool
	cannotRunOnDaemon bool
	doesNotUseRepo    bool

	// doesNotUseConfigAsInput describes commands that do not use the config as
	// input. These commands either initialize the config or perform operations
	// that don't require access to the config.
	//
	// pre-command hooks that require configs must not be run before these
	// commands.
	doesNotUseConfigAsInput bool

	// preemptsAutoUpdate describes commands that must be executed without the
	// auto-update pre-command hook
	preemptsAutoUpdate bool
}

func (d *cmdDetails) String() string {
	return fmt.Sprintf("on client? %t, on daemon? %t, uses repo? %t",
		d.canRunOnClient(), d.canRunOnDaemon(), d.usesRepo())
}

func (d *cmdDetails) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"canRunOnClient":     d.canRunOnClient(),
		"canRunOnDaemon":     d.canRunOnDaemon(),
		"preemptsAutoUpdate": d.preemptsAutoUpdate,
		"usesConfigAsInput":  d.usesConfigAsInput(),
		"usesRepo":           d.usesRepo(),
	}
}

func (d *cmdDetails) usesConfigAsInput() bool { return !d.doesNotUseConfigAsInput }
func (d *cmdDetails) canRunOnClient() bool    { return !d.cannotRunOnClient }
func (d *cmdDetails) canRunOnDaemon() bool    { return !d.cannotRunOnDaemon }
func (d *cmdDetails) usesRepo() bool          { return !d.doesNotUseRepo }

// "What is this madness!?" you ask. Our commands have the unfortunate problem of
// not being able to run on all the same contexts. This map describes these
// properties so that other code can make decisions about whether to invoke a
// command or return an error to the user.
var cmdDetailsMap = map[*cmds.Command]cmdDetails{
	initCmd: {doesNotUseConfigAsInput: true, cannotRunOnDaemon: true, doesNotUseRepo: true},

	// daemonCmd allows user to initialize the config. Thus, it may be called
	// without using the config as input
	daemonCmd:                             {doesNotUseConfigAsInput: true, cannotRunOnDaemon: true},
	commandsClientCmd:                     {doesNotUseRepo: true},
	commands.CommandsDaemonCmd:            {doesNotUseRepo: true},
	commands.VersionCmd:                   {doesNotUseConfigAsInput: true, doesNotUseRepo: true}, // must be permitted to run before init
	commands.LogCmd:                       {cannotRunOnClient: true},
	commands.ActiveReqsCmd:                {cannotRunOnClient: true},
	commands.RepoFsckCmd:                  {cannotRunOnDaemon: true},
	commands.ConfigCmd.Subcommand("edit"): {cannotRunOnDaemon: true, doesNotUseRepo: true},
}
