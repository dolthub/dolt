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
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strings"

	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	filesys2 "github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var NoRemote = Remote{}

var ErrBranchDoesNotMatchUpstream = errors.New("the upstream branch of your current branch does not match the name of your current branch")
var ErrFailedToReadDb = errors.New("failed to read from the db")
var ErrUnknownBranch = errors.New("unknown branch")
var ErrCannotSetUpstreamForTag = errors.New("cannot set upstream for tag")
var ErrCannotPushRef = errors.New("cannot push ref")
var ErrNoRefSpecForRemote = errors.New("no refspec for remote")
var ErrInvalidFetchSpec = errors.New("invalid fetch spec")
var ErrPullWithRemoteNoUpstream = errors.New("You asked to pull from the remote '%s', but did not specify a branch. Because this is not the default configured remote for your current branch, you must specify a branch.")
var ErrPullWithNoRemoteAndNoUpstream = errors.New("There is no tracking information for the current branch.\nPlease specify which branch you want to merge with.\n\n\tdolt pull <remote> <branch>\n\nIf you wish to set tracking information for this branch you can do so with:\n\n\t dolt push --set-upstream <remote> <branch>\n")

var ErrCurrentBranchHasNoUpstream = goerrors.NewKind("fatal: The current branch %s has no upstream branch.\n" +
	"To push the current branch and set the remote as upstream, use\n" +
	"\tdolt push --set-upstream %s %s\n" +
	"To have this happen automatically for branches without a tracking\n" +
	"upstream, see 'push.autoSetupRemote' in 'dolt config --help'.")
var ErrInvalidRepository = goerrors.NewKind("fatal: remote '%s' not found.\n" +
	"Please make sure the remote exists.")
var ErrAllFlagCannotBeUsedWithRefSpec = goerrors.NewKind("fatal: --all can't be combined with refspecs")
var ErrNoPushDestination = goerrors.NewKind("fatal: No configured push destination.\n" +
	"Either specify the URL from the command-line or configure a remote repository using\n\n" +
	"\tdolt remote add <name> <url>\n\n" +
	"and then push using the remote name\n\n" +
	"\tdolt push <name>\n\n")
var ErrFailedToPush = goerrors.NewKind("error: failed to push some refs to '%s'\n" +
	"hint: Updates were rejected because the tip of your current branch is behind\n" +
	"hint: its remote counterpart. Integrate the remote changes (e.g.\n" +
	"hint: 'dolt pull ...') before pushing again.\n")

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

func (r *Remote) GetRemoteDB(ctx context.Context, nbf *types.NomsBinFormat, dialer dbfactory.GRPCDialProvider) (*doltdb.DoltDB, error) {
	params := make(map[string]interface{})
	for k, v := range r.Params {
		params[k] = v
	}

	params[dbfactory.GRPCDialProviderParam] = dialer

	return doltdb.LoadDoltDBWithParams(ctx, nbf, r.Url, filesys2.LocalFS, params)
}

// Prepare does whatever work is necessary to prepare the remote given to receive pushes. Not all remote types can
// support this operations and must be prepared manually. For existing remotes, no work is done.
func (r *Remote) Prepare(ctx context.Context, nbf *types.NomsBinFormat, dialer dbfactory.GRPCDialProvider) error {
	params := make(map[string]interface{})
	for k, v := range r.Params {
		params[k] = v
	}

	params[dbfactory.GRPCDialProviderParam] = dialer

	return dbfactory.PrepareDB(ctx, nbf, r.Url, params)
}

func (r *Remote) GetRemoteDBWithoutCaching(ctx context.Context, nbf *types.NomsBinFormat, dialer dbfactory.GRPCDialProvider) (*doltdb.DoltDB, error) {
	params := make(map[string]interface{})
	for k, v := range r.Params {
		params[k] = v
	}
	params[dbfactory.NoCachingParameter] = "true"
	params[dbfactory.GRPCDialProviderParam] = dialer

	return doltdb.LoadDoltDBWithParams(ctx, nbf, r.Url, filesys2.LocalFS, params)
}

func (r Remote) WithParams(params map[string]string) Remote {
	fetchSpecs := make([]string, len(r.FetchSpecs))
	copy(fetchSpecs, r.FetchSpecs)
	for k, v := range r.Params {
		params[k] = v
	}
	r.Params = params
	return r
}

// PushOptions contains information needed for push for
// one or more branches or a tag for a specific remote database.
type PushOptions struct {
	Targets []*PushTarget
	Rsr     RepoStateReader
	Rsw     RepoStateWriter
	Remote  *Remote
	SrcDb   *doltdb.DoltDB
	DestDb  *doltdb.DoltDB
	TmpDir  string
}

// PushTarget contains information needed for push per branch or tag.
type PushTarget struct {
	SrcRef      ref.DoltRef
	DestRef     ref.DoltRef
	RemoteRef   ref.DoltRef
	Mode        ref.UpdateMode
	SetUpstream bool
	HasUpstream bool
}

func NewPushOpts(ctx context.Context, apr *argparser.ArgParseResults, rsr RepoStateReader, ddb *doltdb.DoltDB, force, setUpstream, pushAutoSetupRemote, all bool) ([]*PushTarget, *Remote, error) {
	if apr.NArg() == 0 {
		return getPushTargetsAndRemoteFromNoArg(ctx, rsr, ddb, force, setUpstream, pushAutoSetupRemote, all)
	}

	rsrBranches, err := rsr.GetBranches()
	if err != nil {
		return nil, nil, err
	}

	currentBranch, err := rsr.CWBHeadRef()
	if err != nil {
		return nil, nil, err
	}

	// the first argument defines the remote name
	remoteName := apr.Arg(0)
	if apr.NArg() == 1 {
		remote, err := getRemote(rsr, remoteName)
		if err != nil {
			return nil, nil, err
		}

		if all {
			return getPushTargetsAndRemoteForAllBranches(ctx, rsrBranches, currentBranch, &remote, ddb, force, setUpstream)
		} else {
			defaultRemote, err := GetDefaultRemote(rsr)
			if err != nil {
				return nil, nil, err
			}

			refSpec, _, hasUpstream, err := getCurrentBranchRefSpec(ctx, rsrBranches, rsr, ddb, remoteName, defaultRemote.Name == remoteName, true, setUpstream, pushAutoSetupRemote)
			if err != nil {
				return nil, nil, err
			}

			opts, err := getPushTargetFromRefSpec(refSpec, currentBranch, &remote, force, setUpstream, hasUpstream)
			if err != nil {
				return nil, nil, err
			}
			return []*PushTarget{opts}, &remote, nil
		}
	} else {
		if all {
			return nil, nil, ErrAllFlagCannotBeUsedWithRefSpec.New()
		}

		refSpecNames := apr.Args[1:]
		// validate given refSpec names
		for _, refSpecName := range refSpecNames {
			if len(refSpecName) == 0 {
				return nil, nil, fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, refSpecName)
			}
		}

		remote, err := getRemote(rsr, apr.Arg(0))
		if err != nil {
			return nil, nil, err
		}
		return getPushTargetsAndRemoteForBranchRefs(ctx, rsrBranches, refSpecNames, currentBranch, &remote, ddb, force, setUpstream)
	}
}

func getRemote(rsr RepoStateReader, name string) (Remote, error) {
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return NoRemote, err
	}

	remote, ok := remotes.Get(name)
	if !ok {
		return NoRemote, ErrInvalidRepository.New(name)
	}
	return remote, nil
}

// getPushTargetsAndRemoteFromNoArg pushes the current branch on default remote if upstream is set or `-u` is defined;
// otherwise, all branches of default remote if `--all` flag is used.
func getPushTargetsAndRemoteFromNoArg(ctx context.Context, rsr RepoStateReader, ddb *doltdb.DoltDB, force, setUpstream, pushAutoSetupRemote, all bool) ([]*PushTarget, *Remote, error) {
	rsrBranches, err := rsr.GetBranches()
	if err != nil {
		return nil, nil, err
	}

	currentBranch, err := rsr.CWBHeadRef()
	if err != nil {
		return nil, nil, err
	}

	remote, err := GetDefaultRemote(rsr)
	if err != nil {
		if err == ErrNoRemote {
			err = ErrNoPushDestination.New()
		}
		return nil, nil, err
	}
	if all {
		return getPushTargetsAndRemoteForAllBranches(ctx, rsrBranches, currentBranch, &remote, ddb, force, setUpstream)
	} else {
		refSpec, remoteName, hasUpstream, err := getCurrentBranchRefSpec(ctx, rsrBranches, rsr, ddb, remote.Name, true, false, setUpstream, pushAutoSetupRemote)
		if err != nil {
			return nil, nil, err
		}
		if remoteName != remote.Name {
			remote, err = getRemote(rsr, remoteName)
			if err != nil {
				return nil, nil, err
			}
		}

		opts, err := getPushTargetFromRefSpec(refSpec, currentBranch, &remote, force, setUpstream, hasUpstream)
		if err != nil {
			return nil, nil, err
		}
		return []*PushTarget{opts}, &remote, nil
	}
}

func getPushTargetsAndRemoteForAllBranches(ctx context.Context, rsrBranches *concurrentmap.Map[string, BranchConfig], currentBranch ref.DoltRef, remote *Remote, ddb *doltdb.DoltDB, force, setUpstream bool) ([]*PushTarget, *Remote, error) {
	localBranches, err := ddb.GetBranches(ctx)
	if err != nil {
		return nil, nil, err
	}
	var lbNames = make([]string, len(localBranches))
	for i, branch := range localBranches {
		lbNames[i] = branch.GetPath()
	}
	return getPushTargetsAndRemoteForBranchRefs(ctx, rsrBranches, lbNames, currentBranch, remote, ddb, force, setUpstream)
}

func getPushTargetsAndRemoteForBranchRefs(ctx context.Context, rsrBranches *concurrentmap.Map[string, BranchConfig], localBranches []string, currentBranch ref.DoltRef, remote *Remote, ddb *doltdb.DoltDB, force, setUpstream bool) ([]*PushTarget, *Remote, error) {
	var pushOptsList []*PushTarget
	for _, refSpecName := range localBranches {
		refSpec, err := getRefSpecFromStr(ctx, ddb, refSpecName)
		if err != nil {
			return nil, nil, err
		}

		// if the remote of upstream does not match the remote given,
		// it should push to the given remote creating new remote branch
		upstream, hasUpstream := rsrBranches.Get(refSpecName)
		hasUpstream = hasUpstream && upstream.Remote == remote.Name

		opts, err := getPushTargetFromRefSpec(refSpec, currentBranch, remote, force, setUpstream, hasUpstream)
		if err != nil {
			return nil, nil, err
		}

		pushOptsList = append(pushOptsList, opts)
	}
	return pushOptsList, remote, nil
}

func getPushTargetFromRefSpec(refSpec ref.RefSpec, currentBranch ref.DoltRef, remote *Remote, force, setUpstream, hasUpstream bool) (*PushTarget, error) {
	src := refSpec.SrcRef(currentBranch)
	dest := refSpec.DestRef(src)

	var remoteRef ref.DoltRef
	var err error
	switch src.GetType() {
	case ref.BranchRefType:
		remoteRef, err = GetTrackingRef(dest, *remote)
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

	return &PushTarget{
		SrcRef:    src,
		DestRef:   dest,
		RemoteRef: remoteRef,
		Mode: ref.UpdateMode{
			Force: force,
		},
		SetUpstream: setUpstream,
		HasUpstream: hasUpstream,
	}, nil
}

// getCurrentBranchRefSpec is called when refSpec is NOT specified. Whether to push depends on the specified remote.
// If the specified remote is the default or the only remote, then it cannot push without its upstream set.
// If the specified remote is one of many and non-default remote, then it pushes regardless of upstream is set.
// If there is no remote specified, the current branch needs to have upstream set to push; otherwise, returns error.
// This function returns |refSpec| for current branch, name of the remote the branch is associated with and
// whether the current branch has upstream set.
func getCurrentBranchRefSpec(ctx context.Context, branches *concurrentmap.Map[string, BranchConfig], rsr RepoStateReader, ddb *doltdb.DoltDB, remoteName string, isDefaultRemote, remoteSpecified, setUpstream, pushAutoSetupRemote bool) (ref.RefSpec, string, bool, error) {
	var refSpec ref.RefSpec
	currentBranch, err := rsr.CWBHeadRef()
	if err != nil {
		return nil, "", false, err
	}

	currentBranchName := currentBranch.GetPath()
	upstream, hasUpstream := branches.Get(currentBranchName)

	if remoteSpecified || pushAutoSetupRemote {
		if isDefaultRemote && !pushAutoSetupRemote {
			return nil, "", false, ErrCurrentBranchHasNoUpstream.New(currentBranchName, remoteName, currentBranchName)
		}
		setUpstream = true
		refSpec, err = getRefSpecFromStr(ctx, ddb, currentBranchName)
		if err != nil {
			return nil, "", false, err
		}
	} else if hasUpstream {
		remoteName = upstream.Remote
		refSpec, err = getCurrentBranchRefSpecFromUpstream(currentBranch, upstream)
		if err != nil {
			return nil, "", false, err
		}
	} else {
		return nil, "", false, ErrCurrentBranchHasNoUpstream.New(currentBranchName, remoteName, currentBranchName)
	}
	return refSpec, remoteName, hasUpstream && upstream.Remote == remoteName, nil
}

// RemoteForFetchArgs returns the remote and remaining arg strings for a fetch command
func RemoteForFetchArgs(args []string, rsr RepoStateReader) (Remote, []string, error) {
	var err error
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return NoRemote, nil, err
	}

	if remotes.Len() == 0 {
		return NoRemote, nil, ErrNoRemote
	}

	var remName string
	if len(args) == 0 {
		remName = "origin"
	} else {
		remName = args[0]
		args = args[1:]
	}

	remote, ok := remotes.Get(remName)
	if !ok {
		msg := "does not appear to be a dolt database. could not read from the remote database. please make sure you have the correct access rights and the database exists"
		return NoRemote, nil, fmt.Errorf("%w; '%s' %s", ErrUnknownRemote, remName, msg)
	}

	return remote, args, nil
}

// ParseRefSpecs returns the ref specs for the string arguments given for the remote provided, or the default ref
// specs for that remote if no arguments are provided. In the event that the default ref specs are returned, the
// returned boolean value will be true.
func ParseRefSpecs(args []string, rsr RepoStateReader, remote Remote) ([]ref.RemoteRefSpec, bool, error) {
	if len(args) != 0 {
		specs, err := ParseRSFromArgs(remote.Name, args)
		return specs, false, err
	} else {
		specs, err := GetRefSpecs(rsr, remote.Name)
		return specs, true, err
	}
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

// getRefSpecFromStr returns ref.RefSpec object using given branch/refSpec name
func getRefSpecFromStr(ctx context.Context, ddb *doltdb.DoltDB, refSpecStr string) (ref.RefSpec, error) {
	refSpecStr, err := disambiguateRefSpecStr(ctx, ddb, refSpecStr)
	if err != nil {
		return nil, err
	}

	refSpec, err := ref.ParseRefSpec(refSpecStr)
	if err != nil {
		return nil, fmt.Errorf("%w: '%s'", err, refSpecStr)
	}

	return refSpec, nil
}

// getCurrentBranchRefSpecFromUpstream validates the number of args defined and returns ref.RefSpec object of
// current branch corresponding to the given upstream.
func getCurrentBranchRefSpecFromUpstream(currentBranch ref.DoltRef, upstream BranchConfig) (ref.RefSpec, error) {
	if currentBranch.GetPath() != upstream.Merge.Ref.GetPath() {
		return nil, ErrBranchDoesNotMatchUpstream
	}

	refSpec, _ := ref.NewBranchToBranchRefSpec(currentBranch.(ref.BranchRef), upstream.Merge.Ref.(ref.BranchRef))
	return refSpec, nil
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
	Squash     bool
	NoFF       bool
	NoCommit   bool
	NoEdit     bool
	Force      bool
	RemoteName string
	Remote     Remote
	RefSpecs   []ref.RemoteRefSpec
	Branch     ref.DoltRef
}

type PullSpecOpt func(*PullSpec)

func WithSquash(squash bool) PullSpecOpt {
	return func(ps *PullSpec) {
		ps.Squash = squash
	}
}

func WithNoFF(noff bool) PullSpecOpt {
	return func(ps *PullSpec) {
		ps.NoFF = noff
	}
}

func WithNoCommit(nocommit bool) PullSpecOpt {
	return func(ps *PullSpec) {
		ps.NoCommit = nocommit
	}
}

func WithNoEdit(noedit bool) PullSpecOpt {
	return func(ps *PullSpec) {
		ps.NoEdit = noedit
	}
}

func WithForce(force bool) PullSpecOpt {
	return func(ps *PullSpec) {
		ps.Force = force
	}
}

// NewPullSpec returns a PullSpec for the arguments given. This function validates remote and gets remoteRef
// for given remoteRefName; if it's not defined, it uses current branch to get its upstream branch if it exists.
func NewPullSpec(
	_ context.Context,
	rsr RepoStateReader,
	remoteName, remoteRefName string,
	remoteOnly bool,
	opts ...PullSpecOpt,
) (*PullSpec, error) {
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
	remote, found := remotes.Get(refSpecs[0].GetRemote())
	if !found {
		return nil, ErrPullWithNoRemoteAndNoUpstream
	}

	var remoteRef ref.DoltRef
	if remoteRefName == "" {
		branch, err := rsr.CWBHeadRef()
		if err != nil {
			return nil, err
		}
		trackedBranches, err := rsr.GetBranches()
		if err != nil {
			return nil, err
		}

		trackedBranch, hasUpstream := trackedBranches.Get(branch.GetPath())
		if !hasUpstream {
			if remoteOnly {
				return nil, fmt.Errorf(ErrPullWithRemoteNoUpstream.Error(), remoteName)
			} else {
				return nil, ErrPullWithNoRemoteAndNoUpstream
			}
		}

		remoteRef = trackedBranch.Merge.Ref
	} else {
		remoteRef = ref.NewBranchRef(remoteRefName)
	}

	spec := &PullSpec{
		RemoteName: remoteName,
		Remote:     remote,
		RefSpecs:   refSpecs,
		Branch:     remoteRef,
	}

	for _, opt := range opts {
		opt(spec)
	}

	return spec, nil
}

func GetAbsRemoteUrl(fs filesys2.Filesys, cfg config.ReadableConfig, urlArg string) (string, string, error) {
	u, err := earl.Parse(urlArg)
	if err != nil {
		return "", "", err
	}

	if u.Scheme != "" && fs != nil {
		if u.Scheme == dbfactory.FileScheme || u.Scheme == dbfactory.LocalBSScheme {
			absUrl, err := getAbsFileRemoteUrl(u, fs)

			if err != nil {
				return "", "", err
			}

			return u.Scheme, absUrl, err
		}

		return u.Scheme, urlArg, nil
	} else if u.Host != "" {
		return dbfactory.HTTPSScheme, "https://" + urlArg, nil
	}

	hostName, err := cfg.GetString(config.RemotesApiHostKey)

	if err != nil {
		if err != config.ErrConfigParamNotFound {
			return "", "", err
		}

		hostName = DefaultRemotesApiHost
	}

	hostName = strings.TrimSpace(hostName)

	return dbfactory.HTTPSScheme, "https://" + path.Join(hostName, u.Path), nil
}

func getAbsFileRemoteUrl(u *url.URL, fs filesys2.Filesys) (string, error) {
	urlStr := u.Host + u.Path
	scheme := u.Scheme

	var err error
	urlStr = filepath.Clean(urlStr)
	urlStr, err = fs.Abs(urlStr)

	if err != nil {
		return "", err
	}

	exists, isDir := fs.Exists(urlStr)

	if !exists {
		// TODO: very odd that GetAbsRemoteUrl will create a directory if it doesn't exist.
		//  This concern should be separated
		err = fs.MkDirs(urlStr)
		if err != nil {
			return "", fmt.Errorf("failed to create directory '%s': %w", urlStr, err)
		}
	} else if !isDir {
		return "", filesys2.ErrIsFile
	}

	urlStr = filepath.ToSlash(urlStr)
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

// SetRemoteUpstreamForRefSpec set upstream for given RefSpec, remote name and branch ref. It uses given RepoStateWriter
// to persist upstream tracking branch information.
func SetRemoteUpstreamForRefSpec(rsw RepoStateWriter, refSpec ref.RefSpec, remote string, branchRef ref.DoltRef) error {
	src := refSpec.SrcRef(branchRef)
	dest := refSpec.DestRef(src)

	return rsw.UpdateBranch(branchRef.GetPath(), BranchConfig{
		Merge: ref.MarshalableRef{
			Ref: dest,
		},
		Remote: remote,
	})
}
