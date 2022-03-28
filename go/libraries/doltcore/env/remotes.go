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
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
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
var ErrInvalidSetUpstreamArgs = errors.New("invalid set-upstream arguments")
var ErrInvalidFetchSpec = errors.New("invalid fetch spec")

func IsEmptyRemote(r Remote) bool {
	return len(r.Name) == 0 && len(r.Url) == 0 && r.FetchSpecs == nil && r.Params == nil
}

type Remote struct {
	Name       string            `json:"name"`
	Url        string            `json:"url"`
	FetchSpecs []string          `json:"fetch_specs"`
	Params     map[string]string `json:"params"`
	dialer     dbfactory.GRPCDialProvider
}

func NewRemote(name, url string, params map[string]string, dialer dbfactory.GRPCDialProvider) Remote {
	return Remote{name, url, []string{"refs/heads/*:refs/remotes/" + name + "/*"}, params, dialer}
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
	params := make(map[string]interface{})
	for k, v := range r.Params {
		params[k] = v
	}
	if r.dialer != nil {
		params[dbfactory.GRPCDialProviderParam] = r.dialer
	}
	return doltdb.LoadDoltDBWithParams(ctx, nbf, r.Url, filesys2.LocalFS, params)
}

func (r *Remote) GetRemoteDBWithoutCaching(ctx context.Context, nbf *types.NomsBinFormat) (*doltdb.DoltDB, error) {
	params := make(map[string]interface{})
	for k, v := range r.Params {
		params[k] = v
	}
	params[dbfactory.NoCachingParameter] = "true"
	if r.dialer != nil {
		params[dbfactory.GRPCDialProviderParam] = r.dialer
	}
	return doltdb.LoadDoltDBWithParams(ctx, nbf, r.Url, filesys2.LocalFS, params)
}

type PushOpts struct {
	SrcRef      ref.DoltRef
	DestRef     ref.DoltRef
	RemoteRef   ref.DoltRef
	Remote      Remote
	Mode        ref.UpdateMode
	SetUpstream bool
}

func NewPushOpts(ctx context.Context, apr *argparser.ArgParseResults, rsr RepoStateReader, ddb *doltdb.DoltDB, force bool, setUpstream bool) (*PushOpts, error) {
	var err error
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return nil, err
	}

	remoteName := "origin"

	args := apr.Args
	if len(args) == 1 {
		if _, ok := remotes[args[0]]; ok {
			remoteName = args[0]
			args = []string{}
		}
	}

	remote, remoteOK := remotes[remoteName]
	currentBranch := rsr.CWBHeadRef()
	branches, err := rsr.GetBranches()
	if err != nil {
		return nil, err
	}
	upstream, hasUpstream := branches[currentBranch.GetPath()]

	var refSpec ref.RefSpec
	if remoteOK && len(args) == 1 {
		refSpecStr := args[0]

		refSpecStr, err = disambiguateRefSpecStr(ctx, ddb, refSpecStr)
		if err != nil {
			return nil, err
		}

		refSpec, err = ref.ParseRefSpec(refSpecStr)
		if err != nil {
			return nil, fmt.Errorf("%w: '%s'", err, refSpecStr)
		}
	} else if len(args) == 2 {
		remoteName = args[0]
		refSpecStr := args[1]

		refSpecStr, err = disambiguateRefSpecStr(ctx, ddb, refSpecStr)
		if err != nil {
			return nil, err
		}

		refSpec, err = ref.ParseRefSpec(refSpecStr)
		if err != nil {
			return nil, fmt.Errorf("%w: '%s'", err, refSpecStr)
		}
	} else if setUpstream {
		return nil, ErrInvalidSetUpstreamArgs
	} else if hasUpstream {
		if len(args) > 0 {
			return nil, fmt.Errorf("%w for '%s'", ErrUpstreamBranchAlreadySet, currentBranch)

		}

		if currentBranch.GetPath() != upstream.Merge.Ref.GetPath() {
			return nil, ErrBranchDoesNotMatchUpstream
		}

		remoteName = upstream.Remote
		refSpec, _ = ref.NewBranchToBranchRefSpec(currentBranch.(ref.BranchRef), upstream.Merge.Ref.(ref.BranchRef))
	} else {
		if len(args) == 0 {
			return nil, ErrNoUpstreamForBranch
		}

		return nil, errors.New("unknown error for remote push args")
	}

	remote, remoteOK = remotes[remoteName]

	if !remoteOK {
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownRemote, remoteName)
	}

	hasRef, err := ddb.HasRef(ctx, currentBranch)

	if err != nil {
		return nil, ErrFailedToReadDb
	} else if !hasRef {
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownBranch, currentBranch.GetPath())

	}

	src := refSpec.SrcRef(currentBranch)
	dest := refSpec.DestRef(src)

	var remoteRef ref.DoltRef

	switch src.GetType() {
	case ref.BranchRefType:
		remoteRef, err = GetTrackingRef(dest, remote)
	case ref.TagRefType:
		if setUpstream {
			err = ErrCannotSetUpstreamForTag
		}
	default:
		err = fmt.Errorf("%w: '%s' of type '%s'", ErrCannotPushRef, src.String(), src.GetType())
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
			Force: force,
		},
		SetUpstream: setUpstream,
	}

	return opts, nil
}

func NewFetchOpts(args []string, rsr RepoStateReader) (Remote, []ref.RemoteRefSpec, error) {
	var err error
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return NoRemote, nil, err
	}

	if len(remotes) == 0 {
		return NoRemote, nil, ErrNoRemote
	}

	var remName string
	if len(args) == 0 {
		remName = "origin"
	} else {
		remName = args[0]
		args = args[1:]
	}

	remote, ok := remotes[remName]
	if !ok {
		msg := "does not appear to be a dolt database. could not read from the remote database. please make sure you have the correct access rights and the database exists"
		return NoRemote, nil, fmt.Errorf("%w; '%s' %s", ErrUnknownRemote, remName, msg)
	}

	var rs []ref.RemoteRefSpec
	if len(args) != 0 {
		rs, err = ParseRSFromArgs(remName, args)
	} else {
		rs, err = GetRefSpecs(rsr, remName)
	}

	if err != nil {
		return NoRemote, nil, err
	}

	return remote, rs, err
}

func ParseRSFromArgs(remName string, args []string) ([]ref.RemoteRefSpec, error) {
	var refSpecs []ref.RemoteRefSpec
	for i := 0; i < len(args); i++ {
		rsStr := args[i]
		rs, err := ref.ParseRefSpec(rsStr)

		if err != nil {
			return nil, fmt.Errorf("%w: '%s'", ErrInvalidFetchSpec, rsStr)
		}

		if _, ok := rs.(ref.BranchToBranchRefSpec); ok {
			local := "refs/heads/" + rsStr
			remTracking := "remotes/" + remName + "/" + rsStr
			rs2, err := ref.ParseRefSpec(local + ":" + remTracking)

			if err == nil {
				rs = rs2
			}
		}

		if rrs, ok := rs.(ref.RemoteRefSpec); !ok {
			return nil, fmt.Errorf("%w: '%s'", ErrInvalidFetchSpec, rsStr)

		} else {
			refSpecs = append(refSpecs, rrs)
		}
	}

	return refSpecs, nil
}

// if possible, convert refs to full spec names. prefer branches over tags.
// eg "main" -> "refs/heads/main", "v1" -> "refs/tags/v1"
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
			return nil, fmt.Errorf("%w '%s' for remote '%s'", ErrInvalidFetchSpec, fsStr, remote.Name)
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

func NewPullSpec(ctx context.Context, rsr RepoStateReader, remoteName string, squash, noff, force bool) (*PullSpec, error) {
	branch := rsr.CWBHeadRef()

	refSpecs, err := GetRefSpecs(rsr, remoteName)
	if err != nil {
		return nil, err
	}

	if len(refSpecs) == 0 {
		return nil, ErrNoRefSpecForRemote
	}

	remotes, err := rsr.GetRemotes()
	if err != nil {
		return nil, err
	}
	remote := remotes[refSpecs[0].GetRemote()]

	return &PullSpec{
		Squash:     squash,
		Noff:       noff,
		RemoteName: remoteName,
		Remote:     remote,
		RefSpecs:   refSpecs,
		Branch:     branch,
		Force:      force,
	}, nil
}

func GetAbsRemoteUrl(fs filesys2.Filesys, cfg config.ReadableConfig, urlArg string) (string, string, error) {
	u, err := earl.Parse(urlArg)

	if err != nil {
		return "", "", err
	}

	if u.Scheme != "" {
		if u.Scheme == dbfactory.FileScheme || u.Scheme == dbfactory.LocalBSScheme {
			absUrl, err := getAbsFileRemoteUrl(u.Host+u.Path, u.Scheme, fs)

			if err != nil {
				return "", "", err
			}

			return u.Scheme, absUrl, err
		}

		return u.Scheme, urlArg, nil
	} else if u.Host != "" {
		return dbfactory.HTTPSScheme, "https://" + urlArg, nil
	}

	hostName, err := cfg.GetString(RemotesApiHostKey)

	if err != nil {
		if err != config.ErrConfigParamNotFound {
			return "", "", err
		}

		hostName = DefaultRemotesApiHost
	}

	hostName = strings.TrimSpace(hostName)

	return dbfactory.HTTPSScheme, "https://" + path.Join(hostName, u.Path), nil
}

func getAbsFileRemoteUrl(urlStr string, scheme string, fs filesys2.Filesys) (string, error) {
	var err error
	urlStr = filepath.Clean(urlStr)
	urlStr, err = fs.Abs(urlStr)

	if err != nil {
		return "", err
	}

	exists, isDir := fs.Exists(urlStr)

	if !exists {
		return "", filesys2.ErrDirNotExist
	} else if !isDir {
		return "", filesys2.ErrIsFile
	}

	urlStr = strings.ReplaceAll(urlStr, `\`, "/")
	if !strings.HasPrefix(urlStr, "/") {
		urlStr = "/" + urlStr
	}
	return scheme + "://" + urlStr, nil
}

// GetDefaultBranch returns the default branch from among the branches given, returning
// the configs default config branch first, then init branch main, then the old init branch master,
// and finally the first lexicographical branch if none of the others are found
func GetDefaultBranch(dEnv *DoltEnv, branches []ref.DoltRef) string {
	if len(branches) == 0 {
		return DefaultInitBranch
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].GetPath() < branches[j].GetPath()
	})

	branchMap := make(map[string]ref.DoltRef)
	for _, b := range branches {
		branchMap[b.GetPath()] = b
	}

	if _, ok := branchMap[DefaultInitBranch]; ok {
		return DefaultInitBranch
	}
	if _, ok := branchMap["master"]; ok {
		return "master"
	}

	// todo: do we care about this during clone?
	defaultOrMain := GetDefaultInitBranch(dEnv.Config)
	if _, ok := branchMap[defaultOrMain]; ok {
		return defaultOrMain
	}

	return branches[0].GetPath()
}
