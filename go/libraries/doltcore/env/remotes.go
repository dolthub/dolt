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

package env

import (
	"context"
	"errors"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var NoRemote = Remote{}

var ErrBranchDoesNotMatchUpstream = errors.New("the upstream branch of your current branch does not match the nane if your current branch")
var ErrUpstreamBranchAlreadySet = errors.New("upstream branch already set")
var ErrNoUpstreamForBranch = errors.New("the current branch has no upstream branch")
var ErrFailedToReadDb = errors.New("failed to read from the db")
var ErrUnknownBranch = errors.New("unknown branch")
var ErrCannotSetUpstreamForTag = errors.New("cannot set upstream for tag")
var ErrCannotPushRef = errors.New("cannot push ref")
var ErrNoRefSpecForRemote = errors.New("no refspec for remote")

func IsEmptyRemote(r Remote) bool {
	return len(r.Name) == 0 && len(r.Url) == 0 && r.FetchSpecs == nil && r.Params == nil
}

type Remote struct {
	Name       string            `json:"name"`
	Url        string            `json:"url"`
	FetchSpecs []string          `json:"fetch_specs"`
	Params     map[string]string `json:"params"`
}

func NewRemote(name, url string, params map[string]string) Remote {
	return Remote{name, url, []string{"refs/heads/*:refs/remotes/" + name + "/*"}, params}
}

func (r *Remote) GetParam(pName string) (string, bool) {
	val, ok := r.Params[pName]
	return val, ok
}

func (r *Remote) GetParamOrDefault(pName, defVal string) string {
	val, ok := r.Params[pName]

	if !ok {
		return defVal
	}

	return val
}

func (r *Remote) GetRemoteDB(ctx context.Context, nbf *types.NomsBinFormat) (*doltdb.DoltDB, error) {
	return doltdb.LoadDoltDBWithParams(ctx, nbf, r.Url, filesys2.LocalFS, r.Params)
}

func (r *Remote) GetRemoteDBWithoutCaching(ctx context.Context, nbf *types.NomsBinFormat) (*doltdb.DoltDB, error) {
	params := make(map[string]string)
	for k, v := range r.Params {
		params[k] = v
	}
	params[dbfactory.NoCachingParameter] = "true"
	return doltdb.LoadDoltDBWithParams(ctx, nbf, r.Url, filesys2.LocalFS, params)
}

const (
	SetUpstreamFlag = "set-upstream"
	ForcePushFlag   = "force"
)

type PushOpts struct {
	SrcRef      ref.DoltRef
	DestRef     ref.DoltRef
	RemoteRef   ref.DoltRef
	Remote      Remote
	Mode        ref.UpdateMode
	SetUpstream bool
}

func ParsePushArgs(ctx context.Context, apr *argparser.ArgParseResults, dEnv *DoltEnv) (*PushOpts, error) {
	var err error
	remotes, err := dEnv.GetRemotes()

	if err != nil {
		//return nil, errhand.BuildDError("error: failed to read remotes from config.").Build()
		return nil, err
	}

	remoteName := "origin"

	args := apr.Args()
	if len(args) == 1 {
		if _, ok := remotes[args[0]]; ok {
			remoteName = args[0]
			args = []string{}
		}
	}

	remote, remoteOK := remotes[remoteName]
	currentBranch := dEnv.RepoStateReader().CWBHeadRef()
	upstream, hasUpstream := dEnv.RepoState.Branches[currentBranch.GetPath()]

	var refSpec ref.RefSpec
	if remoteOK && len(args) == 1 {
		refSpecStr := args[0]

		refSpecStr, err = disambiguateRefSpecStr(ctx, dEnv.DoltDB, refSpecStr)
		if err != nil {
			return nil, err
		}

		refSpec, err = ref.ParseRefSpec(refSpecStr)

		if err != nil {
			//verr = errhand.BuildDError("error: invalid refspec '%s'", refSpecStr).AddCause(err).Build()
			return nil, err
		}
	} else if len(args) == 2 {
		remoteName = args[0]
		refSpecStr := args[1]

		refSpecStr, err = disambiguateRefSpecStr(ctx, dEnv.DoltDB, refSpecStr)
		if err != nil {
			//verr = errhand.VerboseErrorFromError(err)
			return nil, err
		}

		refSpec, err = ref.ParseRefSpec(refSpecStr)
		if err != nil {
			//verr = errhand.BuildDError("error: invalid refspec '%s'", refSpecStr).AddCause(err).Build()
			return nil, err
		}
	} else if apr.Contains(SetUpstreamFlag) {
		//verr = errhand.BuildDError("error: --set-upstream requires <remote> and <refspec> params.").SetPrintUsage().Build()
		return nil, err
	} else if hasUpstream {
		if len(args) > 0 {
			//return nil, errhand.BuildDError("fatal: upstream branch set for '%s'.  Use 'dolt push' without arguments to push.\n", currentBranch).Build()
			return nil, ErrUpstreamBranchAlreadySet

		}

		if currentBranch.GetPath() != upstream.Merge.Ref.GetPath() {
			//return nil, errhand.BuildDError("fatal: The upstream branch of your current branch does not match"+
			//	"the name of your current branch.  To push to the upstream branch\n"+
			//	"on the remote, use\n\n"+
			//	"\tdolt push origin HEAD: %s\n\n"+
			//	"To push to the branch of the same name on the remote, use\n\n"+
			//	"\tdolt push origin HEAD",
			//	currentBranch.GetPath()).Build()
			return nil, ErrBranchDoesNotMatchUpstream
		}

		remoteName = upstream.Remote
		refSpec, _ = ref.NewBranchToBranchRefSpec(currentBranch.(ref.BranchRef), upstream.Merge.Ref.(ref.BranchRef))
	} else {
		if len(args) == 0 {
			remoteName = "<remote>"
			if defRemote, verr := dEnv.GetDefaultRemote(); verr == nil {
				remoteName = defRemote.Name
			}
			return nil, ErrNoUpstreamForBranch
			//return nil, errhand.BuildDError("fatal: The current branch " + currentBranch.GetPath() + " has no upstream branch.\n" +
			//	"To push the current branch and set the remote as upstream, use\n" +
			//	"\tdolt push --set-upstream " + remoteName + " " + currentBranch.GetPath()).Build()
		}

		//verr = errhand.BuildDError("").SetPrintUsage().Build()
		return nil, errors.New("unknown error for remote push args")
	}

	remote, remoteOK = remotes[remoteName]

	if !remoteOK {
		//return nil, errhand.BuildDError("fatal: unknown remote " + remoteName).Build()
		return nil, ErrUnknownRemote
	}

	hasRef, err := dEnv.DoltDB.HasRef(ctx, currentBranch)

	if err != nil {
		//return nil, errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
		return nil, ErrFailedToReadDb
	} else if !hasRef {
		//return nil, errhand.BuildDError("fatal: unknown branch " + currentBranch.GetPath()).Build()
		return nil, ErrUnknownBranch
	}

	src := refSpec.SrcRef(currentBranch)
	dest := refSpec.DestRef(src)

	var remoteRef ref.DoltRef

	switch src.GetType() {
	case ref.BranchRefType:
		remoteRef, err = GetTrackingRef(dest, remote)
	case ref.TagRefType:
		if apr.Contains(SetUpstreamFlag) {
			//verr = errhand.BuildDError("cannot set upstream for tag").Build()
			err = ErrCannotSetUpstreamForTag
		}
	default:
		//verr = errhand.BuildDError("cannot push ref %s of type %s", src.String(), src.GetType()).Build()
		err = ErrCannotPushRef
	}

	if err != nil {
		return nil, err
	}

	opts := &PushOpts{
		SrcRef:    src,
		DestRef:   dest,
		RemoteRef: remoteRef,
		Remote:    remote,
		Mode: ref.UpdateMode{
			Force: apr.Contains(ForcePushFlag),
		},
		SetUpstream: apr.Contains(SetUpstreamFlag),
	}

	return opts, nil
}

// if possible, convert refs to full spec names. prefer branches over tags.
// eg "master" -> "refs/heads/master", "v1" -> "refs/tags/v1"
func disambiguateRefSpecStr(ctx context.Context, ddb *doltdb.DoltDB, refSpecStr string) (string, error) {
	brachRefs, err := ddb.GetBranches(ctx)

	if err != nil {
		return "", err
	}

	for _, br := range brachRefs {
		if br.GetPath() == refSpecStr {
			return br.String(), nil
		}
	}

	tagRefs, err := ddb.GetTags(ctx)

	if err != nil {
		return "", err
	}

	for _, tr := range tagRefs {
		if tr.GetPath() == refSpecStr {
			return tr.String(), nil
		}
	}

	return refSpecStr, nil
}

func GetTrackingRef(branchRef ref.DoltRef, remote Remote) (ref.DoltRef, error) {
	for _, fsStr := range remote.FetchSpecs {
		fs, err := ref.ParseRefSpecForRemote(remote.Name, fsStr)

		if err != nil {
			//return nil, errhand.BuildDError("error: invalid fetch spec '%s' for remote '%s'", fsStr, remote.Name).Build()
			return nil, err
		}

		remoteRef := fs.DestRef(branchRef)

		if remoteRef != nil {
			return remoteRef, nil
		}
	}

	return nil, nil
}

type PullSpec struct {
	Msg        string
	Squash     bool
	Noff       bool
	Force      bool
	RemoteName string
	Remote     Remote
	RefSpecs   []ref.RemoteRefSpec
	Branch     ref.DoltRef
}

func ParsePullSpec(ctx context.Context, dEnv *DoltEnv, remoteName, msg string, squash bool, noff bool, force bool) (*PullSpec, error) {
	branch := dEnv.RepoStateReader().CWBHeadRef()

	refSpecs, err := dEnv.GetRefSpecs(remoteName)
	if err != nil {
		return nil, err
	}

	if len(refSpecs) == 0 {
		//return errhand.BuildDError("error: no refspec for remote").Build()
		return nil, ErrNoRefSpecForRemote
	}

	remote := dEnv.RepoState.Remotes[refSpecs[0].GetRemote()]

	return &PullSpec{
		Msg:        msg,
		Squash:     squash,
		Noff:       noff,
		RemoteName: remoteName,
		Remote:     remote,
		RefSpecs:   refSpecs,
		Branch:     branch,
		Force:      force,
	}, nil
}

type MergeSpec struct {
	H1           hash.Hash
	H2           hash.Hash
	Cm1          *doltdb.Commit
	Cm2          *doltdb.Commit
	TblNames     []string
	WorkingDiffs map[string]hash.Hash
	Squash       bool
	Msg          string
	Noff         bool
	Force        bool
	AllowEmpty   bool
	Email        string
	Name         string
	Date         time.Time
}

func ParseMergeSpec(ctx context.Context, dEnv *DoltEnv, msg string, commitSpecStr string, squash bool, noff bool, force bool, date time.Time) (*MergeSpec, bool, error) {
	cs1, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		//return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
		return nil, false, err
	}

	cm1, err := dEnv.DoltDB.Resolve(context.TODO(), cs1, dEnv.RepoStateReader().CWBHeadRef())

	cs2, err := doltdb.NewCommitSpec(commitSpecStr)
	if err != nil {
		//return nil, errhand.BuildDError("'%s' is not a valid commit", cSpecStr).Build()
		return nil, false, err
	}

	cm2, err := dEnv.DoltDB.Resolve(context.TODO(), cs2, dEnv.RepoStateReader().CWBHeadRef())

	h1, err := cm1.HashOf()

	if err != nil {
		//return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
		return nil, false, err
	}

	h2, err := cm2.HashOf()

	if err != nil {
		//return errhand.BuildDError("error: failed to get hash of commit").AddCause(err).Build()
		return nil, false, err

	}

	if h1 == h2 {
		return nil, false, err
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return nil, false, err
	}

	tblNames, workingDiffs, err := merge.MergeWouldStompChanges(ctx, roots, cm2)
	if err != nil {
		return nil, false, err
		//return errhand.BuildDError("error: failed to determine mergability.").AddCause(err).Build()
	}

	name, email, err := GetNameAndEmail(dEnv.Config)
	if err != nil {
		//verr = errhand.BuildDError("error: committing").AddCause(err).Build()
		//return handleCommitErr(ctx, dEnv, verr, usage)
		return nil, false, err
	}

	return &MergeSpec{
		H1:           h1,
		H2:           h2,
		Cm1:          cm1,
		Cm2:          cm2,
		TblNames:     tblNames,
		WorkingDiffs: workingDiffs,
		Squash:       squash,
		Msg:          msg,
		Noff:         noff,
		Force:        force,
		Email:        email,
		Name:         name,
		Date:         date,
	}, true, nil
}

// GetNameAndEmail returns the name and email from the supplied config
func GetNameAndEmail(cfg config.ReadableConfig) (string, string, error) {
	name, err := cfg.GetString(UserNameKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrNameNotConfigured
	} else if err != nil {
		return "", "", err
	}

	email, err := cfg.GetString(UserEmailKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrEmailNotConfigured
	} else if err != nil {
		return "", "", err
	}

	return name, email, nil
}
