// Copyright 2019-2020 Dolthub, Inc.
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
	"crypto/tls"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	DefaultLoginUrl       = "https://dolthub.com/settings/credentials"
	DefaultMetricsHost    = "eventsapi.dolthub.com"
	DefaultMetricsPort    = "443"
	DefaultRemotesApiHost = "doltremoteapi.dolthub.com"
	DefaultRemotesApiPort = "443"
	tempTablesDir         = "temptf"
)

var ErrPreexistingDoltDir = errors.New(".dolt dir already exists")
var ErrStateUpdate = errors.New("error updating local data repo state")
var ErrMarshallingSchema = errors.New("error marshalling schema")
var ErrInvalidCredsFile = errors.New("invalid creds file")

// DoltEnv holds the state of the current environment used by the cli.
type DoltEnv struct {
	Version string

	Config     *DoltCliConfig
	CfgLoadErr error

	RepoState *RepoState
	RSLoadErr error

	Docs        doltdocs.Docs
	DocsLoadErr error

	DoltDB      *doltdb.DoltDB
	DBLoadError error

	FS     filesys.Filesys
	urlStr string
	hdp    HomeDirProvider
}

// Load loads the DoltEnv for the current directory of the cli
func Load(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, urlStr, version string) *DoltEnv {
	config, cfgErr := loadDoltCliConfig(hdp, fs)
	repoState, rsErr := LoadRepoState(fs)
	docs, docsErr := doltdocs.LoadDocs(fs)
	ddb, dbLoadErr := doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr)

	dEnv := &DoltEnv{
		version,
		config,
		cfgErr,
		repoState,
		rsErr,
		docs,
		docsErr,
		ddb,
		dbLoadErr,
		fs,
		urlStr,
		hdp,
	}

	if dbLoadErr == nil && dEnv.HasDoltDir() {
		if !dEnv.HasDoltTempTableDir() {
			err := dEnv.FS.MkDirs(dEnv.TempTableFilesDir())
			dEnv.DBLoadError = err
		} else {
			// fire and forget cleanup routine.  Will delete as many old temp files as it can during the main commands execution.
			// The process will not wait for this to finish so this may not always complete.
			go func() {
				_ = fs.Iter(dEnv.TempTableFilesDir(), true, func(path string, size int64, isDir bool) (stop bool) {
					if !isDir {
						lm, exists := fs.LastModified(path)

						if exists && time.Now().Sub(lm) > (time.Hour*24) {
							_ = fs.DeleteFile(path)
						}
					}

					return false
				})
			}()
		}
	}

	dbfactory.InitializeFactories(dEnv)

	return dEnv
}

// HasDoltDir returns true if the .dolt directory exists and is a valid directory
func (dEnv *DoltEnv) HasDoltDir() bool {
	return dEnv.hasDoltDir("./")
}

func (dEnv *DoltEnv) HasDoltDataDir() bool {
	exists, isDir := dEnv.FS.Exists(dbfactory.DoltDataDir)
	return exists && isDir
}

func (dEnv *DoltEnv) HasDoltTempTableDir() bool {
	ex, _ := dEnv.FS.Exists(dEnv.TempTableFilesDir())

	return ex
}

func mustAbs(dEnv *DoltEnv, path ...string) string {
	absPath, err := dEnv.FS.Abs(filepath.Join(path...))

	if err != nil {
		panic(err)
	}

	return absPath
}

// GetDoltDir returns the path to the .dolt directory
func (dEnv *DoltEnv) GetDoltDir() string {
	if !dEnv.HasDoltDataDir() {
		panic("No dolt dir")
	}

	return mustAbs(dEnv, dbfactory.DoltDir)
}

func (dEnv *DoltEnv) hasDoltDir(path string) bool {
	exists, isDir := dEnv.FS.Exists(mustAbs(dEnv, dbfactory.DoltDir))
	return exists && isDir
}

// HasLocalConfig returns true if a repository local config file
func (dEnv *DoltEnv) HasLocalConfig() bool {
	_, ok := dEnv.Config.GetConfig(LocalConfig)

	return ok
}

func (dEnv *DoltEnv) bestEffortDeleteAll(dir string) {
	fileToIsDir := make(map[string]bool)
	dEnv.FS.Iter(dir, false, func(path string, size int64, isDir bool) (stop bool) {
		fileToIsDir[path] = isDir
		return false
	})

	for path, isDir := range fileToIsDir {
		if isDir {
			dEnv.FS.Delete(path, true)
		} else {
			dEnv.FS.DeleteFile(path)
		}
	}
}

// InitRepo takes an empty directory and initializes it with a .dolt directory containing repo state, uncommitted license and readme, and creates a noms
// database with dolt structure.
func (dEnv *DoltEnv) InitRepo(ctx context.Context, nbf *types.NomsBinFormat, name, email string) error { // should remove name and email args
	return dEnv.InitRepoWithTime(ctx, nbf, name, email, doltdb.CommitNowFunc())
}

func (dEnv *DoltEnv) InitRepoWithTime(ctx context.Context, nbf *types.NomsBinFormat, name, email string, t time.Time) error { // should remove name and email args
	doltDir, err := dEnv.createDirectories(".")

	if err != nil {
		return err
	}

	err = dEnv.configureRepo(doltDir)

	if err == nil {
		err = dEnv.InitDBAndRepoState(ctx, nbf, name, email, t)
	}

	if err != nil {
		dEnv.bestEffortDeleteAll(dbfactory.DoltDir)
	}

	return err
}

func (dEnv *DoltEnv) InitRepoWithNoData(ctx context.Context, nbf *types.NomsBinFormat) error {
	doltDir, err := dEnv.createDirectories(".")

	if err != nil {
		return err
	}

	err = dEnv.configureRepo(doltDir)

	if err != nil {
		dEnv.bestEffortDeleteAll(dbfactory.DoltDir)
		return err
	}

	dEnv.DoltDB, err = doltdb.LoadDoltDB(ctx, nbf, dEnv.urlStr)

	return err
}

func (dEnv *DoltEnv) createDirectories(dir string) (string, error) {
	absPath, err := dEnv.FS.Abs(dir)

	if err != nil {
		return "", err
	}

	exists, isDir := dEnv.FS.Exists(absPath)

	if !exists {
		return "", fmt.Errorf("'%s' does not exist so could not create '%s", absPath, dbfactory.DoltDataDir)
	} else if !isDir {
		return "", fmt.Errorf("'%s' exists but it's a file not a directory", absPath)
	}

	if dEnv.hasDoltDir(dir) {
		return "", ErrPreexistingDoltDir
	}

	absDataDir := filepath.Join(absPath, dbfactory.DoltDataDir)
	err = dEnv.FS.MkDirs(absDataDir)

	if err != nil {
		return "", fmt.Errorf("unable to make directory '%s', cause: %s", absDataDir, err.Error())
	}

	err = dEnv.FS.MkDirs(dEnv.TempTableFilesDir())

	if err != nil {
		return "", fmt.Errorf("unable to make directory '%s', cause: %s", dEnv.TempTableFilesDir(), err.Error())
	}

	return filepath.Join(absPath, dbfactory.DoltDir), nil
}

func (dEnv *DoltEnv) configureRepo(doltDir string) error {
	err := dEnv.Config.CreateLocalConfig(map[string]string{})

	if err != nil {
		return fmt.Errorf("failed creating file %s", getLocalConfigPath())
	}

	return nil
}

// Inits the dolt DB of this environment with an empty commit at the time given and writes default docs to disk.
// Writes new repo state with a master branch and current root hash.
func (dEnv *DoltEnv) InitDBAndRepoState(ctx context.Context, nbf *types.NomsBinFormat, name, email string, t time.Time) error {
	err := dEnv.InitDBWithTime(ctx, nbf, name, email, t)
	if err != nil {
		return err
	}

	return dEnv.InitializeRepoState(ctx)
}

// Inits the dolt DB of this environment with an empty commit at the time given and writes default docs to disk.
// Does not update repo state.
func (dEnv *DoltEnv) InitDBWithTime(ctx context.Context, nbf *types.NomsBinFormat, name, email string, t time.Time) error {
	var err error
	dEnv.DoltDB, err = doltdb.LoadDoltDB(ctx, nbf, dEnv.urlStr)

	if err != nil {
		return err
	}

	err = dEnv.DoltDB.WriteEmptyRepoWithCommitTime(ctx, name, email, t)
	if err != nil {
		return doltdb.ErrNomsIO
	}

	return nil
}

// InitializeRepoState writes a default repo state to disk, consisting of a master branch and current root hash value.
func (dEnv *DoltEnv) InitializeRepoState(ctx context.Context) error {
	cs, _ := doltdb.NewCommitSpec(doltdb.MasterBranch)
	commit, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
	if err != nil {
		return err
	}

	root, err := commit.GetRootValue()
	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}

	// TODO: stop reading repo state
	dEnv.RepoState, err = CreateRepoState(dEnv.FS, doltdb.MasterBranch, rootHash)
	if err != nil {
		return ErrStateUpdate
	}

	err = dEnv.UpdateWorkingRoot(ctx, root)
	if err != nil {
		return err
	}

	dEnv.RSLoadErr = nil
	return nil
}

// WorkingRoot returns the working root for the current working branch
func (dEnv *DoltEnv) WorkingRoot(ctx context.Context) (*doltdb.RootValue, error) {
	workingSet, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return nil, err
	}

	return workingSet.RootValue(), nil
}

func (dEnv *DoltEnv) WorkingSet(ctx context.Context) (*doltdb.WorkingSet, error) {
	workingSetRef, err := ref.WorkingSetRefForHead(dEnv.RepoState.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	workingSet, err := dEnv.DoltDB.ResolveWorkingSet(ctx, workingSetRef)
	if err != nil {
		return nil, err
	}

	return workingSet, nil
}

// UpdateWorkingRoot updates the working root for the current working branch to the root value given.
// This method can fail if another client updates the working root at the same time.
func (dEnv *DoltEnv) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	var h hash.Hash
	var wsRef ref.WorkingSetRef

	ws, err := dEnv.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(dEnv.RepoState.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	// TODO: add actual trace logging here
	//logrus.Infof("Updating working root to %s", newRoot.DebugString(context.Background(), true))

	return dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, ws.WithWorkingRoot(newRoot), h)
}

// UpdateWorkingSet updates the working set for the current working branch to the value given.
// This method can fail if another client updates the working set at the same time.
func (dEnv *DoltEnv) UpdateWorkingSet(ctx context.Context, ws *doltdb.WorkingSet) error {
	h, err := ws.HashOf()
	if err != nil {
		return err
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, ws.Ref(), ws, h)
}

type repoStateReader struct {
	dEnv *DoltEnv
}

func (r *repoStateReader) WorkingRoot(ctx context.Context) (*doltdb.RootValue, error) {
	return r.dEnv.WorkingRoot(ctx)
}

func (r *repoStateReader) CWBHeadRef() ref.DoltRef {
	return r.dEnv.RepoState.CWBHeadRef()
}

func (r *repoStateReader) CWBHeadSpec() *doltdb.CommitSpec {
	return r.dEnv.RepoState.CWBHeadSpec()
}

func (r *repoStateReader) CWBHeadHash(ctx context.Context) (hash.Hash, error) {
	ref := r.CWBHeadRef()
	cm, err := r.dEnv.DoltDB.ResolveCommitRef(ctx, ref)

	if err != nil {
		return hash.Hash{}, err
	}

	return cm.HashOf()
}

func (r *repoStateReader) StagedHash() hash.Hash {
	return hash.Parse(r.dEnv.RepoState.staged)
}

func (r *repoStateReader) IsMergeActive() bool {
	return r.dEnv.RepoState.Merge != nil
}

func (r *repoStateReader) GetMergeCommit() string {
	return r.dEnv.RepoState.Merge.Commit
}

func (r *repoStateReader) GetPreMergeWorking() string {
	return r.dEnv.RepoState.Merge.PreMergeWorking
}

func (dEnv *DoltEnv) RepoStateReader() RepoStateReader {
	return &repoStateReader{dEnv}
}

type repoStateWriter struct {
	*DoltEnv
}

func (r *repoStateWriter) SetStagedHash(ctx context.Context, h hash.Hash) error {
	r.RepoState.staged = h.String()
	err := r.RepoState.Save(r.FS)

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (r *repoStateWriter) SetWorkingHash(ctx context.Context, h hash.Hash) error {
	r.RepoState.working = h.String()
	err := r.RepoState.Save(r.FS)

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (r *repoStateWriter) SetCWBHeadRef(ctx context.Context, marshalableRef ref.MarshalableRef) error {
	r.RepoState.Head = marshalableRef
	err := r.RepoState.Save(r.FS)

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (r *repoStateWriter) AbortMerge() error {
	return r.RepoState.AbortMerge(r.FS)
}

func (r *repoStateWriter) ClearMerge() error {
	return r.RepoState.ClearMerge(r.FS)
}

func (r *repoStateWriter) StartMerge(commitStr string) error {
	return r.RepoState.StartMerge(commitStr, r.FS)
}

func (dEnv *DoltEnv) RepoStateWriter() RepoStateWriter {
	return &repoStateWriter{dEnv}
}

type docsReadWriter struct {
	FS filesys.Filesys
}

// GetDocsOnDisk reads the filesystem and returns all docs.
func (d *docsReadWriter) GetDocsOnDisk(docNames ...string) (doltdocs.Docs, error) {
	if docNames != nil {
		ret := make(doltdocs.Docs, len(docNames))

		for i, name := range docNames {
			doc, err := doltdocs.GetDoc(d.FS, name)
			if err != nil {
				return nil, err
			}
			ret[i] = doc
		}

		return ret, nil
	}

	return doltdocs.GetSupportedDocs(d.FS)
}

// WriteDocsToDisk creates or updates the dolt_docs table with docs.
func (d *docsReadWriter) WriteDocsToDisk(docs doltdocs.Docs) error {
	return docs.Save(d.FS)
}

func (dEnv *DoltEnv) DocsReadWriter() DocsReadWriter {
	return &docsReadWriter{dEnv.FS}
}

func (dEnv *DoltEnv) HeadRoot(ctx context.Context) (*doltdb.RootValue, error) {
	commit, err := dEnv.DoltDB.ResolveCommitRef(ctx, dEnv.RepoState.CWBHeadRef())

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue()
}

func (dEnv *DoltEnv) DbData() DbData {
	return DbData{
		Ddb: dEnv.DoltDB,
		Rsw: dEnv.RepoStateWriter(),
		Rsr: dEnv.RepoStateReader(),
		Drw: dEnv.DocsReadWriter(),
	}
}

func (dEnv *DoltEnv) StagedRoot(ctx context.Context) (*doltdb.RootValue, error) {
		workingSet, err := dEnv.WorkingSet(ctx)
		if err != nil {
			return nil, err
		}

		return workingSet.StagedRoot(), nil
}

// UpdateStagedRoot updates the staged root for the current working branch. This can fail if multiple clients attempt
// to update at the same time.
func (dEnv *DoltEnv) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	var h hash.Hash
	var wsRef ref.WorkingSetRef

	ws, err := dEnv.WorkingSet(ctx)
	if err == doltdb.ErrWorkingSetNotFound {
		// first time updating root
		wsRef, err = ref.WorkingSetRefForHead(dEnv.RepoState.CWBHeadRef())
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef)
	} else if err != nil {
		return err
	} else {
		h, err = ws.HashOf()
		if err != nil {
			return err
		}

		wsRef = ws.Ref()
	}

	return dEnv.DoltDB.UpdateWorkingSet(ctx, wsRef, ws.WithWorkingRoot(newRoot), h)
}

// todo: move this out of env to actions
func (dEnv *DoltEnv) PutTableToWorking(ctx context.Context, sch schema.Schema, rows types.Map, indexData types.Map, tableName string, autoVal types.Value) error {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return doltdb.ErrNomsIO
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return ErrMarshallingSchema
	}

	tbl, err := doltdb.NewTable(ctx, vrw, schVal, rows, indexData, autoVal)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, tableName, tbl)
	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}

	newRootHash, err := newRoot.HashOf()
	if err != nil {
		return err
	}
	if rootHash == newRootHash {
		return nil
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}

func (dEnv *DoltEnv) IsMergeActive() bool {
	return dEnv.RepoState.Merge != nil
}

func (dEnv *DoltEnv) GetTablesWithConflicts(ctx context.Context) ([]string, error) {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return nil, err
	}

	return root.TablesInConflict(ctx)
}

func (dEnv *DoltEnv) CredsDir() (string, error) {
	return getCredsDir(dEnv.hdp)
}

func (dEnv *DoltEnv) UserRPCCreds() (creds.DoltCreds, bool, error) {
	kid, err := dEnv.Config.GetString(UserCreds)

	if err == nil && kid != "" {
		dir, err := dEnv.CredsDir()

		if err != nil {
			// not sure why you wouldn't be able to get the creds dir.
			panic(err)
		}

		c, err := creds.JWKCredsReadFromFile(dEnv.FS, filepath.Join(dir, kid+".jwk"))
		return c, c.IsPrivKeyValid() && c.IsPubKeyValid(), err
	}

	return creds.EmptyCreds, false, nil
}

func (dEnv *DoltEnv) getRPCCreds() (credentials.PerRPCCredentials, error) {
	dCreds, valid, err := dEnv.UserRPCCreds()
	if err != nil {
		return nil, ErrInvalidCredsFile
	}
	if !valid {
		return nil, nil
	}
	return dCreds, nil
}

func (dEnv *DoltEnv) getUserAgentString() string {
	tokens := []string{
		"dolt_cli",
		dEnv.Version,
		runtime.GOOS,
		runtime.GOARCH,
	}

	for i, t := range tokens {
		tokens[i] = strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return '_'
			}

			return r
		}, strings.TrimSpace(t))
	}

	return strings.Join(tokens, " ")
}

func (dEnv *DoltEnv) GetGRPCDialParams(config grpcendpoint.Config) (string, []grpc.DialOption, error) {
	endpoint := config.Endpoint
	if strings.IndexRune(endpoint, ':') == -1 {
		if config.Insecure {
			endpoint += ":80"
		} else {
			endpoint += ":443"
		}
	}

	var opts []grpc.DialOption
	if config.Insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		tc := credentials.NewTLS(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(tc))
	}

	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)))
	opts = append(opts, grpc.WithUserAgent(dEnv.getUserAgentString()))

	if config.Creds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(config.Creds))
	} else if config.WithEnvCreds {
		rpcCreds, err := dEnv.getRPCCreds()
		if err != nil {
			return "", nil, err
		}
		if rpcCreds != nil {
			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}
	}

	return endpoint, opts, nil
}

func (dEnv *DoltEnv) GetRemotes() (map[string]Remote, error) {
	if dEnv.RSLoadErr != nil {
		return nil, dEnv.RSLoadErr
	}

	return dEnv.RepoState.Remotes, nil
}

var ErrNotACred = errors.New("not a valid credential key id or public key")

func (dEnv *DoltEnv) FindCreds(credsDir, pubKeyOrId string) (string, error) {
	if !creds.B32CredsByteSet.ContainsAll([]byte(pubKeyOrId)) {
		return "", creds.ErrBadB32CredsEncoding
	}

	if len(pubKeyOrId) == creds.B32EncodedPubKeyLen {
		pubKeyOrId, _ = creds.PubKeyStrToKIDStr(pubKeyOrId)
	}

	if len(pubKeyOrId) != creds.B32EncodedKeyIdLen {
		return "", ErrNotACred
	}

	path := mustAbs(dEnv, credsDir, pubKeyOrId+creds.JWKFileExtension)
	exists, isDir := dEnv.FS.Exists(path)

	if isDir {
		return path, filesys.ErrIsDir
	} else if !exists {
		return "", creds.ErrCredsNotFound
	} else {
		return path, nil
	}
}

func (dEnv *DoltEnv) FindRef(ctx context.Context, refStr string) (ref.DoltRef, error) {
	localRef := ref.NewBranchRef(refStr)
	if hasRef, err := dEnv.DoltDB.HasRef(ctx, localRef); err != nil {
		return nil, err
	} else if hasRef {
		return localRef, nil
	} else {
		if strings.HasPrefix(refStr, "remotes/") {
			refStr = refStr[len("remotes/"):]
		}

		slashIdx := strings.IndexRune(refStr, '/')
		if slashIdx > 0 {
			remoteName := refStr[:slashIdx]
			if _, ok := dEnv.RepoState.Remotes[remoteName]; ok {
				remoteRef, err := ref.NewRemoteRefFromPathStr(refStr)

				if err != nil {
					return nil, err
				}

				if hasRef, err = dEnv.DoltDB.HasRef(ctx, remoteRef); err != nil {
					return nil, err
				} else if hasRef {
					return remoteRef, nil
				}
			}
		}
	}

	return nil, doltdb.ErrBranchNotFound
}

// GetRefSpecs takes an optional remoteName and returns all refspecs associated with that remote.  If "" is passed as
// the remoteName then the default remote is used.
func (dEnv *DoltEnv) GetRefSpecs(remoteName string) ([]ref.RemoteRefSpec, errhand.VerboseError) {
	var remote Remote
	var verr errhand.VerboseError

	if remoteName == "" {
		remote, verr = dEnv.GetDefaultRemote()
	} else if r, ok := dEnv.RepoState.Remotes[remoteName]; ok {
		remote = r
	} else {
		verr = errhand.BuildDError("error: unknown remote '%s'", remoteName).Build()
	}

	if verr != nil {
		return nil, verr
	}

	var refSpecs []ref.RemoteRefSpec
	for _, fs := range remote.FetchSpecs {
		rs, err := ref.ParseRefSpecForRemote(remote.Name, fs)

		if err != nil {
			return nil, errhand.BuildDError("error: for '%s', '%s' is not a valid refspec.", remote.Name, fs).Build()
		}

		if rrs, ok := rs.(ref.RemoteRefSpec); !ok {
			return nil, errhand.BuildDError("error: '%s' is not a valid refspec referring to a remote tracking branch", remote.Name).Build()
		} else if rrs.GetRemote() != remote.Name {
			return nil, errhand.BuildDError("error: remote '%s' refers to remote '%s'", remote.Name, rrs.GetRemote()).Build()
		} else {
			refSpecs = append(refSpecs, rrs)
		}
	}

	return refSpecs, nil
}

var ErrNoRemote = errhand.BuildDError("error: no remote.").Build()
var ErrCantDetermineDefault = errhand.BuildDError("error: unable to determine the default remote.").Build()

// GetDefaultRemote gets the default remote for the environment.  Not fully implemented yet.  Needs to support multiple
// repos and a configurable default.
func (dEnv *DoltEnv) GetDefaultRemote() (Remote, errhand.VerboseError) {
	remotes := dEnv.RepoState.Remotes

	if len(remotes) == 0 {
		return NoRemote, ErrNoRemote
	} else if len(remotes) == 1 {
		for _, v := range remotes {
			return v, nil
		}
	}

	if remote, ok := dEnv.RepoState.Remotes["origin"]; ok {
		return remote, nil
	}

	return NoRemote, ErrCantDetermineDefault
}

// GetUserHomeDir returns the user's home dir
// based on current filesys
func (dEnv *DoltEnv) GetUserHomeDir() (string, error) {
	return getHomeDir(dEnv.hdp)
}

func (dEnv *DoltEnv) TempTableFilesDir() string {
	return mustAbs(dEnv, dEnv.GetDoltDir(), tempTablesDir)
}
