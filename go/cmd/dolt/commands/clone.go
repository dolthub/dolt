package commands

import (
	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"os"
	"path"
	"path/filepath"
)

var cloneShortDesc = ""
var cloneLongDesc = ""
var cloneSynopsis = []string{
	"[-remote <remote>] [-branch <branch>] <remote-url> <new-dir>",
}

func Clone(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString("remote", "", "name", "")
	ap.SupportsString("branch", "b", "branch", "")
	help, usage := cli.HelpAndUsagePrinters(commandStr, cloneShortDesc, cloneLongDesc, cloneSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	remoteName := apr.GetValueOrDefault("remote", "origin")
	branch := apr.GetValueOrDefault("branch", "master")
	dir, urlStr, verr := parseArgs(apr)

	if verr == nil {
		verr = cloneRemote(dir, remoteName, urlStr, branch, dEnv.FS)
	}

	return HandleVErrAndExitCode(verr, usage)
}

func parseArgs(apr *argparser.ArgParseResults) (string, string, errhand.VerboseError) {
	if apr.NArg() < 1 || apr.NArg() > 2 {
		return "", "", errhand.BuildDError("Invalid usage").SetPrintUsage().Build()
	}

	urlStr := apr.Arg(0)
	_, err := env.ParseRemoteUrl(urlStr)

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

	dEnv := env.Load(env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB)
	err = dEnv.InitRepoWithNoData()

	if err != nil {
		return nil, errhand.BuildDError("error: unable to initialize repo without data").AddCause(err).Build()
	}

	return dEnv, nil
}

func createRemote(remoteName, remoteUrl string, dEnv *env.DoltEnv) (*doltdb.DoltDB, errhand.VerboseError) {
	r := &env.Remote{Name: remoteName, Url: remoteUrl}

	cfg, _ := dEnv.Config.GetConfig(env.LocalConfig)
	key := env.RemoteConfigParam(r.Name, env.RemoteUrlParam)
	err := cfg.SetStrings(map[string]string{key: r.Url})

	if err != nil {
		return nil, errhand.BuildDError("error: unable to update local config with new remote " + remoteName).AddCause(err).Build()
	}

	return r.GetRemoteDB(), nil
}

func cloneRemote(dir, remoteName, remoteUrl, branch string, fs filesys.Filesys) errhand.VerboseError {
	dEnv, verr := clonedEnv(dir, fs)

	if verr != nil {
		return verr
	}

	srcDB, verr := createRemote(remoteName, remoteUrl, dEnv)

	if verr != nil {
		return verr
	}

	if !srcDB.HasBranch(branch) {
		return errhand.BuildDError("fatal: unknown branch " + branch).Build()
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", branch)
	cm, err := srcDB.Resolve(cs)

	if err != nil {
		return errhand.BuildDError("error: unable to find %v", branch).Build()
	}

	progChan := make(chan datas.PullProgress)
	stopChan := make(chan struct{})
	go progFunc(progChan, stopChan)

	remoteBranch := path.Join("remotes", remoteName, branch)
	err = actions.Fetch(remoteBranch, srcDB, dEnv.DoltDB, cm, progChan)
	close(progChan)
	<-stopChan

	if err != nil {
		return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
	}

	err = dEnv.DoltDB.NewBranchAtCommit(branch, cm)

	if err != nil {
		return errhand.BuildDError("error: failed to create branch " + branch).Build()
	}

	localCommitSpec, _ := doltdb.NewCommitSpec("HEAD", branch)
	localCommit, _ := dEnv.DoltDB.Resolve(localCommitSpec)
	h, err := dEnv.DoltDB.WriteRootValue(localCommit.GetRootValue())
	dEnv.RepoState, err = env.CreateRepoState(dEnv.FS, branch, h)

	if err != nil {
		return errhand.BuildDError("error: failed to write repo state").Build()
	}

	return nil
}
