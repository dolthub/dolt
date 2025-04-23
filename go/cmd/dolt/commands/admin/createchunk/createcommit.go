// Copyright 2025 Dolthub, Inc.
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

package createchunk

import (
	"bytes"
	"context"
	"errors"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
)

// CreateCommitCmd creates a new commit chunk, printing the new chunk's hash on success.
// The user must supply a branch name, which will be set to this new commit.
// This is only required for the CLI command, and is optional when invoking the equivalent stored procedure.
// This is because the journal must end with a root hash, and is only flushed when there is a new root hash.
// Thus, we must update the root hash before the command finishes, or else changes will not be persisted.
type CreateCommitCmd struct{}

func generateCreateCommitSQL(cliCtx cli.CliContext, apr *argparser.ArgParseResults) (query string, params []interface{}, err error) {
	var buffer bytes.Buffer
	var first bool
	first = true
	buffer.WriteString("CALL DOLT_ADMIN_CREATECHUNK_COMMIT(")

	writeParam := func(key, val string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'--")
		buffer.WriteString(key)
		buffer.WriteString("', ")
		buffer.WriteString("?")
		first = false
		params = append(params, val)
	}

	forwardParam := func(key string) {
		val, ok := apr.GetValue(key)
		if !ok {
			return
		}
		writeParam(key, val)
	}

	forwardFlag := func(flag string) {
		if !apr.Contains(flag) {
			return
		}
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'--")
		buffer.WriteString(flag)
		buffer.WriteString("'")
		first = false
	}

	var author string
	if apr.Contains(cli.AuthorParam) {
		author, _ = apr.GetValue(cli.AuthorParam)
	} else {
		name, email, err := env.GetNameAndEmail(cliCtx.Config())
		if err != nil {
			return "", nil, err
		}
		author = name + " <" + email + ">"
	}
	writeParam(cli.AuthorParam, author)

	forwardParam("desc")
	forwardParam("root")
	forwardParam("parents")
	forwardParam(cli.BranchParam)
	forwardFlag(cli.ForceFlag)

	buffer.WriteString(")")
	return buffer.String(), params, nil
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CreateCommitCmd) Name() string {
	return "commit"
}

// Description returns a description of the command
func (cmd CreateCommitCmd) Description() string {
	return "Creates a new commit chunk in the dolt storage"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd CreateCommitCmd) RequiresRepo() bool {
	return false
}

func (cmd CreateCommitCmd) Docs() *cli.CommandDocumentation {
	// Admin commands are undocumented
	return nil
}

func (cmd CreateCommitCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCreateCommitParser()
}

func (cmd CreateCommitCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))

	// Ensure that the CLI args parse, but only check that a branch was supplied.
	// All other args will be validated in the system procedure, but the branch is only required in the CLI.
	apr := cli.ParseArgsOrDie(ap, args, usage)
	if !apr.Contains(cli.BranchParam) {
		cli.PrintErrf("the --%s flag is required when creating a chunk using the CLI", cli.BranchParam)
		return 1
	}

	desc, _ := apr.GetValue("desc")
	root, _ := apr.GetValue("root")
	parents, _ := apr.GetValueList("parents")
	branch, isBranchSet := apr.GetValue(cli.BranchParam)
	force := apr.Contains(cli.ForceFlag)

	var name, email string
	var err error
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}
	} else {
		name, email, err = env.GetNameAndEmail(cliCtx.Config())
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}
	}

	db := dEnv.DbData(ctx).Ddb
	commitRootHash, ok := hash.MaybeParse(root)
	if !ok {
		cli.PrintErrf("invalid root value hash")
		return 1
	}

	var parentCommits []hash.Hash
	for _, parent := range parents {
		commitSpec, err := doltdb.NewCommitSpec(parent)
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}

		headRef := dEnv.RepoState.CWBHeadRef()

		optionalCommit, err := db.Resolve(ctx, commitSpec, headRef)
		if err != nil {
			cli.PrintErrln(errhand.VerboseErrorFromError(err))
			return 1
		}
		parentCommits = append(parentCommits, optionalCommit.Addr)
	}

	commitMeta, err := datas.NewCommitMeta(name, email, desc)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	// This isn't technically an amend, but the Amend field controls whether the commit must be a child of the ref's current commit (if any)
	commitOpts := datas.CommitOptions{
		Parents: parentCommits,
		Meta:    commitMeta,
		Amend:   force,
	}

	rootVal, err := db.ValueReadWriter().ReadValue(ctx, commitRootHash)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	var commit *doltdb.Commit
	if isBranchSet {
		commit, err = db.CommitValue(ctx, ref.NewBranchRef(branch), rootVal, commitOpts)
		if errors.Is(err, datas.ErrMergeNeeded) {
			cli.PrintErrf("branch %s already exists. If you wish to overwrite it, add the --force flag", branch)
			return 1
		}
	} else {
		commit, err = db.CommitDangling(ctx, rootVal, commitOpts)
	}
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	cli.Println(commitHash.String())

	return 0
}
