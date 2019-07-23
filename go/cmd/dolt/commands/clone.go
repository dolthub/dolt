// Copyright 2019 Liquidata, Inc.
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
	"context"
	"os"
	"path"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/earl"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const (
	remoteParam = "remote"
	branchParam = "branch"
)

var cloneShortDesc = "Clone a data repository into a new directory"
var cloneLongDesc = "Clones a repository into a newly created directory, creates remote-tracking branches for each " +
	"branch in the cloned repository (visible using dolt branch -a), and creates and checks out an initial branch that " +
	"is forked from the cloned repository's currently active branch.\n" +
	"\n" +
	"After the clone, a plain <b>dolt fetch</b> without arguments will update all the remote-tracking branches, and a <b>dolt " +
	"pull</b> without arguments will in addition merge the remote branch into the current branch\n" +
	"\n" +
	"This default configuration is achieved by creating references to the remote branch heads under refs/remotes/origin " +
	"and by creating a remote named 'origin'."
var cloneSynopsis = []string{
	"[-remote <remote>] [-branch <branch>]  [--aws-region <region>] [--aws-creds-type <creds-type>] [--aws-creds-file <file>] [--aws-creds-profile <profile>] <remote-url> <new-dir>",
}

func Clone(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(remoteParam, "", "name", "Name of the remote to be added. Default will be 'origin'.")
	ap.SupportsString(branchParam, "b", "branch", "The branch to be cloned.  If not specified all branches will be cloned.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, credTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file.")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, cloneShortDesc, cloneLongDesc, cloneSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	remoteName := apr.GetValueOrDefault(remoteParam, "origin")
	branch := apr.GetValueOrDefault(branchParam, "")
	dir, urlStr, verr := parseArgs(apr)

	scheme, remoteUrl, err := getAbsRemoteUrl(dEnv.FS, dEnv.Config, urlStr)

	if err != nil {
		verr = errhand.BuildDError("error: '%s' is not valid.", urlStr).Build()
	}

	if verr == nil {
		var params map[string]string
		params, verr = parseRemoteArgs(apr, scheme, remoteUrl)

		if verr == nil {
			var r env.Remote
			var srcDB *doltdb.DoltDB
			r, srcDB, verr = createRemote(remoteName, remoteUrl, params)

			if verr == nil {
				dEnv, verr = envForClone(srcDB.ValueReadWriter().Format(), r, dir, dEnv.FS)

				if verr == nil {
					verr = cloneRemote(context.Background(), srcDB, remoteName, branch, dEnv)

					// Make best effort to delete the directory we created.
					if verr != nil {
						_ = os.Chdir("../")
						_ = dEnv.FS.Delete(dir, true)
					}
				}
			}
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func parseArgs(apr *argparser.ArgParseResults) (string, string, errhand.VerboseError) {
	if apr.NArg() < 1 || apr.NArg() > 2 {
		return "", "", errhand.BuildDError("").SetPrintUsage().Build()
	}

	urlStr := apr.Arg(0)
	_, err := earl.Parse(urlStr)

	if err != nil {
		return "", "", errhand.BuildDError("error: invalid remote url: " + urlStr).Build()
	}

	var dir string
	if apr.NArg() == 2 {
		dir = apr.Arg(1)
	} else {
		dir = path.Base(urlStr)
		if dir == "." {
			dir = path.Dir(urlStr)
		} else if dir == "/" {
			return "", "", errhand.BuildDError("Could not infer repo name.  Please explicitily define a directory for this url").Build()
		}
	}

	return dir, urlStr, nil
}

func envForClone(nbf *types.NomsBinFormat, r env.Remote, dir string, fs filesys.Filesys) (*env.DoltEnv, errhand.VerboseError) {
	exists, _ := fs.Exists(filepath.Join(dir, dbfactory.DoltDir))

	if exists {
		return nil, errhand.BuildDError("error: data repository already exists at " + dir).Build()
	}

	err := fs.MkDirs(dir)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to create directories: " + dir).Build()
	}

	err = os.Chdir(dir)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to access directory " + dir).Build()
	}

	dEnv := env.Load(context.TODO(), env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB)
	err = dEnv.InitRepoWithNoData(context.TODO(), nbf)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to initialize repo without data").AddCause(err).Build()
	}

	dEnv.RSLoadErr = nil
	dEnv.RepoState, err = env.CloneRepoState(dEnv.FS, r)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to create repo state with remote " + r.Name).AddCause(err).Build()
	}

	return dEnv, nil
}

func createRemote(remoteName, remoteUrl string, params map[string]string) (env.Remote, *doltdb.DoltDB, errhand.VerboseError) {
	cli.Printf("cloning %s\n", remoteUrl)

	r := env.NewRemote(remoteName, remoteUrl, params)

	ddb, err := r.GetRemoteDB(context.TODO(), types.Format_Default)

	if err != nil {
		bdr := errhand.BuildDError("error: failed to get remote db").AddCause(err)

		if err == remotestorage.ErrInvalidDoltSpecPath {
			urlObj, _ := earl.Parse(remoteUrl)
			bdr.AddDetails("'%s' should be in the format 'organization/repo'", urlObj.Path)
		}

		return env.NoRemote, nil, bdr.Build()
	}

	return r, ddb, nil
}

func cloneRemote(ctx context.Context, srcDB *doltdb.DoltDB, remoteName, branch string, dEnv *env.DoltEnv) errhand.VerboseError {
	var branches []ref.DoltRef
	if len(branch) > 0 {
		branches = []ref.DoltRef{ref.NewBranchRef(branch)}
	} else {
		var err error
		branches, err = srcDB.GetBranches(ctx)

		if err != nil {
			return errhand.BuildDError("error: failed to read branches").AddCause(err).Build()
		}
	}

	return cloneAllBranchRefs(branches, srcDB, ctx, remoteName, dEnv)
}

func cloneAllBranchRefs(branches []ref.DoltRef, srcDB *doltdb.DoltDB, ctx context.Context, remoteName string, dEnv *env.DoltEnv) errhand.VerboseError {
	var dref ref.DoltRef
	var masterHash hash.Hash
	var h hash.Hash

	for i := 0; i < len(branches); i++ {
		dref = branches[i]
		branch := dref.GetPath()
		hasRef, err := srcDB.HasRef(ctx, dref)

		if err != nil {
			return errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
		}

		if !hasRef {
			return errhand.BuildDError("fatal: unknown branch " + branch).Build()
		}

		cs, _ := doltdb.NewCommitSpec("HEAD", dref.GetPath())
		cm, err := srcDB.Resolve(ctx, cs)

		if err != nil {
			return errhand.BuildDError("error: unable to find %v", branch).AddCause(err).Build()
		}

		progChan := make(chan datas.PullProgress)
		doneChan := make(chan struct{})
		go progFunc(progChan, doneChan)

		remoteBranch := ref.NewRemoteRef(remoteName, branch)
		err = actions.Fetch(ctx, remoteBranch, srcDB, dEnv.DoltDB, cm, progChan)
		close(progChan)
		<-doneChan

		if err != nil {
			return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
		}

		err = dEnv.DoltDB.NewBranchAtCommit(ctx, dref, cm)

		if err != nil {
			return errhand.BuildDError("error: failed to create branch " + branch).AddCause(err).Build()
		}

		localCommitSpec, _ := doltdb.NewCommitSpec("HEAD", branch)
		localCommit, _ := dEnv.DoltDB.Resolve(ctx, localCommitSpec)

		h, err = dEnv.DoltDB.WriteRootValue(ctx, localCommit.GetRootValue())

		if err != nil {
			return errhand.BuildDError("error: failed to write to database.").AddCause(err).Build()
		}

		if branch == "master" {
			masterHash = h
		}
	}

	if !masterHash.IsEmpty() {
		h = masterHash
		dref = ref.NewBranchRef("master")
	}

	dEnv.RepoState.Head = ref.MarshalableRef{Ref: dref}
	dEnv.RepoState.Staged = h.String()
	dEnv.RepoState.Working = h.String()
	err := dEnv.RepoState.Save()

	if err != nil {
		return errhand.BuildDError("error: failed to write repo state").AddCause(err).Build()
	}

	return nil
}

type RpcErrVerbWrap struct {
	*remotestorage.RpcError
}

func (vw RpcErrVerbWrap) ShouldPrintUsage() bool {
	return false
}

func (vw RpcErrVerbWrap) Verbose() string {
	return vw.FullDetails()
}
