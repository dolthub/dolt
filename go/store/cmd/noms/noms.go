// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/attic-labs/kingpin"
	flag "github.com/juju/gnuflag"

	"github.com/dolthub/dolt/go/store/cmd/noms/util"
	"github.com/dolthub/dolt/go/store/util/exit"
	"github.com/dolthub/dolt/go/store/util/profile"
	"github.com/dolthub/dolt/go/store/util/verbose"
)

var commands = []*util.Command{
	nomsConfig,
	nomsDs,
	nomsRoot,
	nomsShow,
	nomsManifest,
	nomsCat,
	nomsWalk,
}

var kingpinCommands = []util.KingpinCommand{
	nomsBlob,
	nomsStats,
}

var actions = []string{
	"interacting with",
	"poking at",
	"goofing with",
	"dancing with",
	"playing with",
	"contemplation of",
	"showing off",
	"jiggerypokery of",
	"singing to",
	"nomming on",
	"fighting with",
}

func usageString() string {
	i := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(actions))
	return fmt.Sprintf(`Noms is a tool for %s Noms data.`, actions[i])
}

func main() {
	// allow short (-h) help
	kingpin.EnableFileExpansion = false
	kingpin.CommandLine.HelpFlag.Short('h')
	noms := kingpin.New("noms", usageString())

	// global flags
	cpuProfileVal := noms.Flag("cpuprofile", "write cpu profile to file").String()
	memProfileVal := noms.Flag("memprofile", "write memory profile to file").String()
	blockProfileVal := noms.Flag("blockprofile", "write block profile to file").String()
	verboseVal := noms.Flag("verbose", "show more").Short('v').Bool()

	// set up docs for non-kingpin commands
	addNomsDocs(noms)

	handlers := map[string]util.KingpinHandler{}

	// install kingpin handlers
	for _, cmdFunction := range kingpinCommands {
		command, handler := cmdFunction(context.Background(), noms)
		handlers[command.FullCommand()] = handler
	}

	input := kingpin.MustParse(noms.Parse(os.Args[1:]))

	// apply global flags
	profile.ApplyProfileFlags(cpuProfileVal, memProfileVal, blockProfileVal)
	verbose.SetVerbose(*verboseVal)

	if handler := handlers[strings.Split(input, " ")[0]]; handler != nil {
		handler(input)
	}

	// fall back to previous (non-kingpin) noms commands

	flag.Parse(false)

	args := flag.Args()

	// Don't prefix log messages with timestamp when running interactively
	log.SetFlags(0)

	for _, cmd := range commands {
		if cmd.Name() == args[0] {
			flags := cmd.Flags()
			flags.Usage = cmd.Usage

			flags.Parse(true, args[1:])
			args = flags.Args()
			if cmd.Nargs != 0 && len(args) < cmd.Nargs {
				cmd.Usage()
			}
			exitCode := cmd.Run(context.Background(), args)
			if exitCode != 0 {
				exit.Exit(exitCode)
			}
			return
		}
	}
}

// addDatabaseArg adds a "database" arg to the passed command
func addDatabaseArg(cmd *kingpin.CmdClause) (arg *string) {
	return cmd.Arg("database", "a noms database path").Required().String() // TODO: custom parser for noms db URL?
}

// addNomsDocs - adds documentation (docs only, not commands) for existing (pre-kingpin) commands.
func addNomsDocs(noms *kingpin.Application) {
	// commit
	commit := noms.Command("commit", `Commits a specified value as head of the dataset
If absolute-path is not provided, then it is read from stdin. See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the dataset and absolute-path arguments.
`)
	commit.Flag("allow-dupe", "creates a new commit, even if it would be identical (modulo metadata and parents) to the existing HEAD.").Default("0").Int()
	commit.Flag("date", "alias for -meta 'date=<date>'. '<date>' must be iso8601-formatted. If '<date>' is empty, it defaults to the current date.").String()
	commit.Flag("message", "alias for -meta 'message=<message>'").String()
	commit.Flag("meta", "'<key>=<value>' - creates a metadata field called 'key' set to 'value'. Value should be human-readable encoded.").String()
	commit.Flag("meta-p", "'<key>=<path>' - creates a metadata field called 'key' set to the value at <path>").String()
	commit.Arg("absolute-path", "the path to read data from").String()
	// TODO: this should be required, but kingpin does not allow required args after non-required ones. Perhaps a custom type would fix that?
	commit.Arg("database", "a noms database path").String()

	// config
	noms.Command("config", "Prints the active configuration if a .nomsconfig file is present")

	// diff
	diff := noms.Command("diff", `Shows the difference between two objects
See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object arguments.
`)
	diff.Flag("stat", "Writes a summary of the changes instead").Short('s').Bool()
	diff.Arg("object1", "").Required().String()
	diff.Arg("object2", "").Required().String()

	// ds
	ds := noms.Command("ds", `Noms dataset management
See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.
`)
	ds.Flag("delete", "dataset to delete").Short('d').String()
	ds.Arg("database", "a noms database path").String()

	// log
	log := noms.Command("log", `Displays the history of a path
See Spelling Values at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the <path-spec> parameter.
`)
	log.Flag("color", "value of 1 forces color on, 0 forces color off").Default("-1").Int()
	log.Flag("max-lines", "max number of lines to show per commit (-1 for all lines)").Default("9").Int()
	log.Flag("max-commits", "max number of commits to display (0 for all commits)").Short('n').Default("0").Int()
	log.Flag("oneline", "show a summary of each commit on a single line").Bool()
	log.Flag("graph", "show ascii-based commit hierarchy on left side of output").Bool()
	log.Flag("show-value", "show commit value rather than diff information").Bool()
	log.Flag("tz", "display formatted date comments in specified timezone, must be: local or utc").Enum("local", "utc")
	log.Arg("path-spec", "").Required().String()

	// merge
	merge := noms.Command("merge", `Merges and commits the head values of two named datasets
See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.
You must provide a working database and the names of two Datasets you want to merge. The values at the heads of these Datasets will be merged, put into a new Commit object, and set as the Head of the third provided Dataset name.
`)
	merge.Flag("policy", "conflict resolution policy for merging. Defaults to 'n', which means no resolution strategy will be applied. Supported values are 'l' (left), 'r' (right) and 'p' (prompt). 'prompt' will bring up a simple command-line prompt allowing you to resolve conflicts by choosing between 'l' or 'r' on a case-by-case basis.").Default("n").Enum("n", "r", "l", "p")
	addDatabaseArg(merge)
	merge.Arg("left-dataset-name", "a dataset").Required().String()
	merge.Arg("right-dataset-name", "a dataset").Required().String()
	merge.Arg("output-dataset-name", "a dataset").Required().String()

	// root
	root := noms.Command("root", `Get or set the current root hash of the entire database
See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the database argument.
`)
	root.Flag("update", "Replaces the entire database with the one with the given hash").String()
	addDatabaseArg(root)

	// show
	show := noms.Command("show", `Shows a serialization of a Noms object
See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.
`)
	show.Flag("raw", "If true, dumps the raw binary version of the data").Bool()
	show.Flag("stats", "If true, reports statistics related to the value").Bool()
	show.Flag("tz", "display formatted date comments in specified timezone, must be: local or utc").Enum("local", "utc")
	show.Arg("object", "a noms object").Required().String()

	// walk
	walk := noms.Command("walk", `Walks references contained in an object.
See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.
`)
	walk.Arg("object", "a noms object").String()
	walk.Flag("quiet", "If true, prints only dangling refs, not the paths of all refs").Bool()

	// version
	noms.Command("version", "Print the noms version")

	//manifest
	manifest := noms.Command("manifest", `Prints a database's manifest in a more readable format`)
	addDatabaseArg(manifest)

	//cat
	cat := noms.Command("cat", `Prints the contents of an nbs file`)
	cat.Arg("nbs-file", "nbs file").Required().String()
	cat.Flag("raw", "If true, includes the raw binary version of each chunk in the nbs file").Bool()
	cat.Flag("decompressed", "If true, includes the decompressed binary version of each chunk in the nbs file").Bool()
	cat.Flag("no-show", "If true, skips printing of the value").Bool()
	cat.Flag("no-refs", "If true, skips printing of the refs").Bool()
	cat.Flag("hashes-only", "If true, only prints the b32 hashes").Bool()
}
