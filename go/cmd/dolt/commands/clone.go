package commands

import (
	"context"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/hash"
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
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
)

const (
	remoteParam = "remote"
	branchParam = "branch"
)

var cloneShortDesc = ""
var cloneLongDesc = ""
var cloneSynopsis = []string{
	"[-remote <remote>] [-branch <branch>] <remote-url> <new-dir>",
}

func Clone(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(remoteParam, "", "name", "Name of the remote to be added. Default will be 'origin'.")
	ap.SupportsString(branchParam, "b", "branch", "The branch to be cloned.  If not specified all branches will be cloned.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, credTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use")
	help, usage := cli.HelpAndUsagePrinters(commandStr, cloneShortDesc, cloneLongDesc, cloneSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	remoteName := apr.GetValueOrDefault(remoteParam, "origin")
	branch := apr.GetValueOrDefault(branchParam, "")
	dir, urlStr, verr := parseArgs(apr)

	scheme, remoteUrl, err := getAbsRemoteUrl(dEnv.Config, urlStr)

	if err != nil {
		verr = errhand.BuildDError("error: '%s' is not valid.", urlStr).Build()
	}

	if verr == nil {
		var params map[string]string
		params, verr = parseRemoteArgs(apr, scheme, remoteUrl)

		if verr == nil {
			dEnv, verr = clonedEnv(dir, dEnv.FS)

			if verr == nil {
				verr = cloneRemote(context.Background(), remoteName, remoteUrl, branch, params, dEnv)

				// Make best effort to delete the directory we created.
				if verr != nil {
					_ = os.Chdir("../")
					_ = dEnv.FS.Delete(dir, true)
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

func clonedEnv(dir string, fs filesys.Filesys) (*env.DoltEnv, errhand.VerboseError) {
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
	err = dEnv.InitRepoWithNoData(context.TODO())

	if err != nil {
		return nil, errhand.BuildDError("error: unable to initialize repo without data").AddCause(err).Build()
	}

	return dEnv, nil
}

func createRemote(remoteName, remoteUrl string, params map[string]string, dEnv *env.DoltEnv) (*doltdb.DoltDB, errhand.VerboseError) {
	cli.Printf("cloning %s\n", remoteUrl)

	r := env.NewRemote(remoteName, remoteUrl, params)

	var err error
	dEnv.RSLoadErr = nil
	dEnv.RepoState, err = env.CloneRepoState(dEnv.FS, r)

	if err != nil {
		return nil, errhand.BuildDError("error: unable to create repo state with remote " + remoteName).AddCause(err).Build()
	}

	ddb, err := r.GetRemoteDB(context.TODO())

	if err != nil {
		bdr := errhand.BuildDError("error: failed to get remote db").AddCause(err)

		if err == remotestorage.ErrInvalidDoltSpecPath {
			urlObj, _ := earl.Parse(remoteUrl)
			bdr.AddDetails("'%s' should be in the format 'organization/repo'", urlObj.Path)
		}

		return nil, bdr.Build()
	}

	return ddb, nil
}

func cloneRemote(ctx context.Context, remoteName, remoteUrl, branch string, params map[string]string, dEnv *env.DoltEnv) (verr errhand.VerboseError) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			verr = remotePanicRecover(r, stack)
		}
	}()

	if verr == nil {
		var srcDB *doltdb.DoltDB
		srcDB, verr = createRemote(remoteName, remoteUrl, params, dEnv)

		if verr == nil {
			var branches []ref.DoltRef
			if len(branch) > 0 {
				branches = []ref.DoltRef{ref.NewBranchRef(branch)}
			} else {
				branches = srcDB.GetBranches(ctx)
			}

			verr = cloneAllBranchRefs(branches, verr, srcDB, ctx, remoteName, dEnv)
		}
	}

	return
}

func cloneAllBranchRefs(branches []ref.DoltRef, verr errhand.VerboseError, srcDB *doltdb.DoltDB, ctx context.Context, remoteName string, dEnv *env.DoltEnv) errhand.VerboseError {
	var dref ref.DoltRef
	var masterHash hash.Hash
	var h hash.Hash
	for i := 0; i < len(branches) && verr == nil; i++ {
		dref = branches[i]
		branch := dref.GetPath()

		if !srcDB.HasRef(ctx, dref) {
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

	dEnv.RepoState.Head = ref.MarshalableRef{dref}
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

func remotePanicRecover(r interface{}, stack []byte) errhand.VerboseError {
	switch val := r.(type) {
	case *remotestorage.RpcError:
		return &RpcErrVerbWrap{val}
	case error:
		return errhand.BuildDError("clone failed").AddCause(val).Build()
	}

	panic(r)
}
