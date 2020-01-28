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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/earl"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/strhelp"
	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/types"
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

type CloneCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CloneCmd) Name() string {
	return "clone"
}

// Description returns a description of the command
func (cmd CloneCmd) Description() string {
	return "Clone from a remote data repository."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd CloneCmd) RequiresRepo() bool {
	return false
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CloneCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, cloneShortDesc, cloneLongDesc, cloneSynopsis, ap)
}

func (cmd CloneCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(remoteParam, "", "name", "Name of the remote to be added. Default will be 'origin'.")
	ap.SupportsString(branchParam, "b", "branch", "The branch to be cloned.  If not specified all branches will be cloned.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, credTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file.")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use.")
	return ap
}

// EventType returns the type of the event to log
func (cmd CloneCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CLONE
}

// Exec executes the command
func (cmd CloneCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
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
			r, srcDB, verr = createRemote(ctx, remoteName, remoteUrl, params)

			if verr == nil {
				dEnv, verr = envForClone(ctx, srcDB.ValueReadWriter().Format(), r, dir, dEnv.FS)

				if verr == nil {
					verr = cloneRemote(ctx, srcDB, remoteName, branch, dEnv)

					if verr == nil {
						evt := events.GetEventFromContext(ctx)
						u, err := earl.Parse(remoteUrl)

						if err == nil {
							if u.Scheme != "" {
								evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
							}
						}
					}

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

func envForClone(ctx context.Context, nbf *types.NomsBinFormat, r env.Remote, dir string, fs filesys.Filesys) (*env.DoltEnv, errhand.VerboseError) {
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

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB)
	err = dEnv.InitRepoWithNoData(ctx, nbf)

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

func createRemote(ctx context.Context, remoteName, remoteUrl string, params map[string]string) (env.Remote, *doltdb.DoltDB, errhand.VerboseError) {
	cli.Printf("cloning %s\n", remoteUrl)

	r := env.NewRemote(remoteName, remoteUrl, params)

	ddb, err := r.GetRemoteDB(ctx, types.Format_Default)

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

func cloneProg(eventCh <-chan datas.TableFileEvent) {
	var (
		chunks            int64
		chunksDownloading int64
		chunksDownloaded  int64
		cliPos            int
	)

	cliPos = cli.DeleteAndPrint(cliPos, "Retrieving remote information.")
	for tblFEvt := range eventCh {
		switch tblFEvt.EventType {
		case datas.Listed:
			for _, tf := range tblFEvt.TableFiles {
				chunks += int64(tf.NumChunks())
			}
		case datas.DownloadStart:
			for _, tf := range tblFEvt.TableFiles {
				chunksDownloading += int64(tf.NumChunks())
			}
		case datas.DownloadSuccess:
			for _, tf := range tblFEvt.TableFiles {
				chunksDownloading -= int64(tf.NumChunks())
				chunksDownloaded += int64(tf.NumChunks())
			}
		case datas.DownloadFailed:
			// Ignore for now and output errors on the main thread
		}

		str := fmt.Sprintf("%s of %s chunks complete. %s chunks being downloaded currently.", strhelp.CommaIfy(chunksDownloaded), strhelp.CommaIfy(chunks), strhelp.CommaIfy(chunksDownloading))
		cliPos = cli.DeleteAndPrint(cliPos, str)
	}

	cli.Println()
}

func cloneRemote(ctx context.Context, srcDB *doltdb.DoltDB, remoteName, branch string, dEnv *env.DoltEnv) errhand.VerboseError {
	wg := &sync.WaitGroup{}
	eventCh := make(chan datas.TableFileEvent, 128)

	wg.Add(1)
	go func() {
		defer wg.Done()
		cloneProg(eventCh)
	}()

	err := actions.Clone(ctx, srcDB, dEnv.DoltDB, eventCh)

	wg.Wait()

	if err != nil {
		return errhand.BuildDError("error: clone failed").AddCause(err).Build()
	}

	if branch == "" {
		branches, err := dEnv.DoltDB.GetBranches(ctx)

		if err != nil {
			return errhand.BuildDError("error: failed to list branches").AddCause(err).Build()
		}

		for _, brnch := range branches {
			branch = brnch.GetPath()

			if branch == doltdb.MasterBranch {
				break
			}
		}
	}

	// If we couldn't find a branch but the repo cloned successfully, it's empty. Initialize it instead of pulling from
	// the remote.
	performPull := true
	if branch == "" {
		err = initEmptyClonedRepo(dEnv, err, ctx)
		if err != nil {
			return nil
		}

		branch = doltdb.MasterBranch
		performPull = false
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", branch)
	cm, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return errhand.BuildDError("error: could not get " + branch).AddCause(err).Build()
	}

	rootVal, err := cm.GetRootValue()
	if err != nil {
		return errhand.BuildDError("error: could not get the root value of " + branch).AddCause(err).Build()
	}

	if performPull {
		remoteRef := ref.NewRemoteRef(remoteName, branch)

		err = dEnv.DoltDB.FastForward(ctx, remoteRef, cm)
		if err != nil {
			return errhand.BuildDError("error: could not create remote ref at " + remoteRef.String()).AddCause(err).Build()
		}

		err = actions.SaveDocsFromRoot(ctx, rootVal, dEnv)
		if err != nil {
			return errhand.BuildDError("error: failed to update docs on the filesystem").AddCause(err).Build()
		}
	}

	h, err := rootVal.HashOf()
	if err != nil {
		return errhand.BuildDError("error: could not get the root value of " + branch).AddCause(err).Build()
	}

	_, err = dEnv.DoltDB.WriteRootValue(ctx, rootVal)
	if err != nil {
		return errhand.BuildDError("error: could not write root value").AddCause(err).Build()
	}

	dEnv.RepoState.Head = ref.MarshalableRef{Ref: ref.NewBranchRef(branch)}
	dEnv.RepoState.Staged = h.String()
	dEnv.RepoState.Working = h.String()

	err = dEnv.RepoState.Save(dEnv.FS)
	if err != nil {
		return errhand.BuildDError("error: failed to write repo state").AddCause(err).Build()
	}

	return nil
}

// Inits an empty, newly cloned repo. This would be unnecessary if we properly initialized the storage for a repository
// when we created it on dolthub. If we do that, this code can be removed.
func initEmptyClonedRepo(dEnv *env.DoltEnv, err error, ctx context.Context) error {
	name := dEnv.Config.GetStringOrDefault(env.UserNameKey, "")
	email := dEnv.Config.GetStringOrDefault(env.UserEmailKey, "")

	if *name == "" {
		return errhand.BuildDError(fmt.Sprintf("error: could not determine user name. run dolt config --global --add %[1]s", env.UserNameKey)).Build()
	} else if *email == "" {
		return errhand.BuildDError("error: could not determine email. run dolt config --global --add %[1]s", env.UserEmailKey).Build()
	}

	err = dEnv.InitDBWithTime(ctx, types.Format_Default, *name, *email, doltdb.CommitNowFunc())
	if err != nil {
		return errhand.BuildDError("error: could not initialize repository").AddCause(err).Build()
	}

	return nil
}
