package commands

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/earl"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"

	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

const (
	insecureFlag = "insecure"
	remoteParam  = "remote"
	branchParam  = "branch"
)

var cloneShortDesc = ""
var cloneLongDesc = ""
var cloneSynopsis = []string{
	"[-remote <remote>] [-branch <branch>] [-insecure] <remote-url> <new-dir>",
}

func Clone(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(insecureFlag, "", "Use an unencrypted connection.")
	ap.SupportsString(remoteParam, "", "name", "Name of the remote to be added. Default will be 'origin'.")
	ap.SupportsString(branchParam, "b", "branch", "The branch to be cloned.  If not specified 'master' will be cloned.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, cloneShortDesc, cloneLongDesc, cloneSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	remoteName := apr.GetValueOrDefault(remoteParam, "origin")
	branch := apr.GetValueOrDefault(branchParam, "master")
	insecure := apr.Contains(insecureFlag)
	dir, urlStr, verr := parseArgs(apr)

	if verr == nil {
		verr = cloneRemote(dir, remoteName, urlStr, branch, insecure, dEnv.FS)
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

func clonedEnv(dir string, fs filesys.Filesys) (*env.DoltEnv, errhand.VerboseError) {
	exists, _ := fs.Exists(filepath.Join(dir, doltdb.DoltDir))

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
	err = dEnv.InitRepoWithNoData(context.TODO())

	if err != nil {
		return nil, errhand.BuildDError("error: unable to initialize repo without data").AddCause(err).Build()
	}

	return dEnv, nil
}

func createRemote(remoteName, remoteUrlIn string, insecure bool, dEnv *env.DoltEnv) (*doltdb.DoltDB, errhand.VerboseError) {
	remoteUrl, err := getAbsRemoteUrl(dEnv.Config, remoteUrlIn)

	if err != nil {
		return nil, errhand.BuildDError("error: '%s' is not valid.", remoteUrlIn).Build()
	}

	cli.Printf("cloning %s\n", remoteUrl)
	r := env.NewRemote(remoteName, remoteUrl, insecure)

	dEnv.RSLoadErr = nil
	dEnv.RepoState, err = env.CloneRepoState(dEnv.FS, r)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to create repo state with remote " + remoteName).AddCause(err).Build()
	}

	return r.GetRemoteDB(context.TODO()), nil
}

func cloneRemote(dir, remoteName, remoteUrl, branch string, insecure bool, fs filesys.Filesys) (verr errhand.VerboseError) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			verr = remotePanicRecover(r, stack)
		}
	}()

	var dEnv *env.DoltEnv
	dEnv, verr = clonedEnv(dir, fs)

	if verr == nil {
		var srcDB *doltdb.DoltDB
		srcDB, verr = createRemote(remoteName, remoteUrl, insecure, dEnv)

		if verr == nil {
			if !srcDB.HasBranch(context.TODO(), branch) {
				verr = errhand.BuildDError("fatal: unknown branch " + branch).Build()
			} else {
				cs, _ := doltdb.NewCommitSpec("HEAD", branch)
				cm, err := srcDB.Resolve(context.TODO(), cs)

				if err != nil {
					verr = errhand.BuildDError("error: unable to find %v", branch).Build()
				} else {

					progChan := make(chan datas.PullProgress)
					stopChan := make(chan struct{})
					go progFunc(progChan, stopChan)

					remoteBranch := path.Join("remotes", remoteName, branch)
					err = actions.Fetch(context.TODO(), remoteBranch, srcDB, dEnv.DoltDB, cm, progChan)
					close(progChan)
					<-stopChan

					if err != nil {
						verr = errhand.BuildDError("error: fetch failed").AddCause(err).Build()
					} else {

						err = dEnv.DoltDB.NewBranchAtCommit(context.TODO(), branch, cm)

						if err != nil {
							verr = errhand.BuildDError("error: failed to create branch " + branch).Build()
						} else {

							localCommitSpec, _ := doltdb.NewCommitSpec("HEAD", branch)
							localCommit, _ := dEnv.DoltDB.Resolve(context.TODO(), localCommitSpec)
							h, err := dEnv.DoltDB.WriteRootValue(context.Background(), localCommit.GetRootValue())

							dEnv.RepoState.Branch = branch
							dEnv.RepoState.Staged = h.String()
							dEnv.RepoState.Working = h.String()
							err = dEnv.RepoState.Save()

							if err != nil {
								verr = errhand.BuildDError("error: failed to write repo state").Build()
							}
						}
					}
				}
			}
		}
	}

	return
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

func remotePanicRecover(r interface{}, stack []byte) errhand.VerboseError {
	switch val := r.(type) {
	case *remotestorage.RpcError:
		return &RpcErrVerbWrap{val}
	case error:
		return errhand.BuildDError("clone failed").AddCause(val).Build()
	}

	panic(r)
}
