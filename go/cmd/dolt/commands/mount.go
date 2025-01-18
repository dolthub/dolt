// Copyright 2024 Dolthub, Inc.
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

package commands

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/mount"
)

type mountOpts struct{}

var mountDocs = cli.CommandDocumentationContent{
	ShortDesc: `Mount Dolt as a file system`,
	LongDesc:  `Mount Dolt as a file system`,
	Synopsis: []string{
		`[{{.LessThan}}mountpoint{{.GreaterThan}}]`,
	},
}

type MountCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MountCmd) Name() string {
	return "mount"
}

// Description returns a description of the command
func (cmd MountCmd) Description() string {
	return `Mount Dolt as a file system`
}

func (cmd MountCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(showDocs, ap)
}

func (cmd MountCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

func (cmd MountCmd) RequiresRepo() bool {
	return true
}

func parseMountArgs(apr *argparser.ArgParseResults) (*mountOpts, error) {
	return &mountOpts{}, nil
}

func (cmd MountCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, mountDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	_, err := parseMountArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	mountpoint := apr.Arg(0)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("dolt"),
		fuse.Subtype("doltfs"),
	)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	defer c.Close()

	err = fs.Serve(c, mount.NewFileSystem(dEnv))
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return 0
}

// TODO: An extra layer on top for selecting the db
// TODO: should we be getting the root set for a branch?
// Different handlers for different message types, which specify different paths.
// Examples:
// Top level can be:
// - head / working / staged
// - a branch
// - a tag
// - an address
// - a remote
// - a fully qualified ref
// - root? for the root store? Kinda redundant
// /workingSets/heads/main/ is a WorkingSet
// /workingSets/heads/main/working is a RootValue
// /refs/heads/main/ is a rootvalue
// /addresses/rt9gl00583v5ulof6qkhun355q6kcpbq is whatever the address resolves to.
// /working, /staged, and /head give you currently checked-out branch
/*
1)       { key: refs/heads/main ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit -> root value
2)       { key: refs/heads/otherBranch ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit
3)       { key: refs/internal/create ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit
4)       { key: refs/remotes/origin/main ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit
5)       { key: refs/tags/aTag ref: #solo3k07o2dc3u4veq0nhklc9il24huk - tag -> commit
6)       { key: workingSets/heads/main ref: #vqthnij64k14fbmgppschunnk5vi4v2b - working set
7)       { key: workingSets/heads/otherBranch ref: #578h6hjd4h0ovp9i1n4hcuts1d2ujp52 - working set
*/

/*
var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return dirDirs, nil
}*/
