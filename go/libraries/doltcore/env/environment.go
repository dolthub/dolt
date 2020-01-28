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

package env

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/creds"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
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
var ErrDocsUpdate = errors.New("error updating local docs")

// DoltEnv holds the state of the current environment used by the cli.
type DoltEnv struct {
	Config     *DoltCliConfig
	CfgLoadErr error

	RepoState *RepoState
	RSLoadErr error

	Docs        Docs
	DocsLoadErr error

	DoltDB      *doltdb.DoltDB
	DBLoadError error

	FS     filesys.Filesys
	urlStr string
	hdp    HomeDirProvider
}

// Load loads the DoltEnv for the current directory of the cli
func Load(ctx context.Context, hdp HomeDirProvider, fs filesys.Filesys, urlStr string) *DoltEnv {
	config, cfgErr := loadDoltCliConfig(hdp, fs)
	repoState, rsErr := LoadRepoState(fs)
	docs, docsErr := LoadDocs(fs)
	ddb, dbLoadErr := doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr)

	dEnv := &DoltEnv{
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
			err := os.Mkdir(dEnv.TempTableFilesDir(), os.ModePerm)
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
	return dEnv.hasDoltDataDir("./")
}

func (dEnv *DoltEnv) HasDoltTempTableDir() bool {
	ex, _ := dEnv.FS.Exists(dEnv.TempTableFilesDir())

	return ex
}

// GetDoltDir returns the path to the .dolt directory
func (dEnv *DoltEnv) GetDoltDir() string {
	if !dEnv.HasDoltDataDir() {
		panic("No dolt dir")
	}
	return filepath.Join("./", dbfactory.DoltDir)
}

func (dEnv *DoltEnv) hasDoltDir(path string) bool {
	exists, isDir := dEnv.FS.Exists(filepath.Join(path, dbfactory.DoltDir))
	return exists && isDir
}

func (dEnv *DoltEnv) hasDoltDataDir(path string) bool {
	exists, isDir := dEnv.FS.Exists(filepath.Join(path, dbfactory.DoltDataDir))
	return exists && isDir
}

// HasLocalConfig returns true if a repository local config file
func (dEnv *DoltEnv) HasLocalConfig() bool {
	_, ok := dEnv.Config.GetConfig(LocalConfig)

	return ok
}

// GetDoc returns the path to the provided file, if it exists
func (dEnv *DoltEnv) GetDoc(file string) string {
	if !hasDocFile(dEnv.FS, file) {
		return ""
	}
	return getDocFile(file)
}

// GetLocalFileText returns a byte slice representing the contents of the provided file, if it exists
func (dEnv *DoltEnv) GetLocalFileText(file string) ([]byte, error) {
	path := dEnv.GetDoc(file)
	if path != "" {
		return dEnv.FS.ReadFile(path)
	}
	return nil, nil
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
	doltDir := filepath.Join(dir, dbfactory.DoltDir)
	if dEnv.hasDoltDir(doltDir) {
		return "", ErrPreexistingDoltDir
	}

	doltDataDir := filepath.Join(doltDir, dbfactory.DataDir)
	err := dEnv.FS.MkDirs(doltDataDir)

	if err != nil {
		return "", fmt.Errorf("unable to make directory %s within the working directory", dbfactory.DoltDataDir)
	}

	return doltDir, nil
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

	return dEnv.initializeRepoState(ctx)
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

	docs, err := CreateDocs(dEnv.FS)
	if err != nil {
		return ErrDocsUpdate
	}
	dEnv.Docs = docs

	return nil
}

// initializeRepoState writes a default repo state to disk, consisting of a master branch and current root hash value.
func (dEnv *DoltEnv) initializeRepoState(ctx context.Context) error {
	cs, _ := doltdb.NewCommitSpec("HEAD", doltdb.MasterBranch)
	commit, _ := dEnv.DoltDB.Resolve(ctx, cs)

	root, err := commit.GetRootValue()
	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()
	if err != nil {
		return err
	}

	dEnv.RepoState, err = CreateRepoState(dEnv.FS, doltdb.MasterBranch, rootHash)
	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (dEnv *DoltEnv) WorkingRoot(ctx context.Context) (*doltdb.RootValue, error) {
	return dEnv.DoltDB.ReadRootValue(ctx, dEnv.RepoState.WorkingHash())
}

func (dEnv *DoltEnv) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	h, err := dEnv.DoltDB.WriteRootValue(ctx, newRoot)

	if err != nil {
		return doltdb.ErrNomsIO
	}

	dEnv.RepoState.Working = h.String()
	err = dEnv.RepoState.Save(dEnv.FS)

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (dEnv *DoltEnv) HeadRoot(ctx context.Context) (*doltdb.RootValue, error) {
	cs, _ := doltdb.NewCommitSpec("head", dEnv.RepoState.Head.Ref.String())
	commit, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue()
}

func (dEnv *DoltEnv) StagedRoot(ctx context.Context) (*doltdb.RootValue, error) {
	return dEnv.DoltDB.ReadRootValue(ctx, dEnv.RepoState.StagedHash())
}

func (dEnv *DoltEnv) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) (hash.Hash, error) {
	h, err := dEnv.DoltDB.WriteRootValue(ctx, newRoot)

	if err != nil {
		return hash.Hash{}, doltdb.ErrNomsIO
	}

	dEnv.RepoState.Staged = h.String()
	err = dEnv.RepoState.Save(dEnv.FS)

	if err != nil {
		return hash.Hash{}, ErrStateUpdate
	}

	return h, nil
}

func (dEnv *DoltEnv) PutTableToWorking(ctx context.Context, rows types.Map, sch schema.Schema, tableName string) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return doltdb.ErrNomsIO
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err := encoding.MarshalAsNomsValue(ctx, vrw, sch)

	if err != nil {
		return ErrMarshallingSchema
	}

	tbl, err := doltdb.NewTable(ctx, vrw, schVal, rows)

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

func (dEnv *DoltEnv) IsUnchangedFromHead(ctx context.Context) (bool, error) {
	root, err := dEnv.HeadRoot(ctx)

	if err != nil {
		return false, err
	}

	headHash, err := root.HashOf()

	if err != nil {
		return false, err
	}

	headHashStr := headHash.String()
	if dEnv.RepoState.Working == headHashStr && dEnv.RepoState.Staged == headHashStr {
		return true, nil
	}

	return false, nil
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

func (dEnv *DoltEnv) GrpcConnWithCreds(hostAndPort string, insecure bool, rpcCreds credentials.PerRPCCredentials) (*grpc.ClientConn, error) {
	if strings.IndexRune(hostAndPort, ':') == -1 {
		if insecure {
			hostAndPort += ":80"
		} else {
			hostAndPort += ":443"
		}
	}

	var dialOpts grpc.DialOption
	if insecure {
		dialOpts = grpc.WithInsecure()
	} else {
		tc := credentials.NewTLS(&tls.Config{})
		dialOpts = grpc.WithTransportCredentials(tc)
	}

	opts := []grpc.DialOption{dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128 * 1024 * 1024))}

	if rpcCreds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
	}

	conn, err := grpc.Dial(hostAndPort, opts...)

	return conn, err
}

func (dEnv *DoltEnv) GrpcConn(hostAndPort string, insecure bool) (*grpc.ClientConn, error) {
	rpcCreds, err := dEnv.getRPCCreds()

	if err != nil {
		return nil, err
	}

	return dEnv.GrpcConnWithCreds(hostAndPort, insecure, rpcCreds)

}

func (dEnv *DoltEnv) GetRemotes() (map[string]Remote, error) {
	if dEnv.RSLoadErr != nil {
		return nil, dEnv.RSLoadErr
	}

	if dEnv.RepoState.Remotes == nil {
		return map[string]Remote{}, nil
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

	path := filepath.Join(credsDir, pubKeyOrId+creds.JWKFileExtension)
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
	return filepath.Join(dEnv.GetDoltDir(), tempTablesDir)
}

func (dEnv *DoltEnv) GetAllValidDocDetails() (docs []doltdb.DocDetails, err error) {
	docs = []doltdb.DocDetails{}
	for _, doc := range *AllValidDocDetails {
		newerText, err := dEnv.GetLocalFileText(doc.File)
		if err != nil {
			return nil, err
		}
		doc.NewerText = newerText
		docs = append(docs, doc)
	}
	return docs, nil
}

func (dEnv *DoltEnv) GetOneDocDetail(docName string) (doc doltdb.DocDetails, err error) {
	for _, doc := range *AllValidDocDetails {
		if doc.DocPk == docName {
			newerText, err := dEnv.GetLocalFileText(doc.File)
			if err != nil {
				return doltdb.DocDetails{}, err
			}
			doc.NewerText = newerText
			return doc, nil
		}
	}
	return doltdb.DocDetails{}, err
}

// GetUpdatedRootWithDocs adds, updates or removes the `dolt_docs` table on the provided root. The table will be added or updated
// When at least one doc.NewerText != nil. If the `dolt_docs` table exists and every doc.NewerText == nil, the table will be removed.
// If no docDetails are provided, we put all valid docs to the working root.
func (dEnv *DoltEnv) GetUpdatedRootWithDocs(ctx context.Context, root *doltdb.RootValue, docDetails []doltdb.DocDetails) (*doltdb.RootValue, error) {
	docTbl, found, err := root.GetTable(ctx, doltdb.DocTableName)

	if err != nil {
		return nil, err
	}

	docDetails, err = getDocDetails(dEnv, docDetails)
	if err != nil {
		return nil, err
	}

	if found {
		return updateDocsOnRoot(ctx, dEnv, root, docTbl, docDetails)
	}
	return createDocsTableOnRoot(ctx, dEnv, root, docDetails)
}

// PutDocsToWorking adds, updates or removes the `dolt_docs` table on the working root using the provided docDetails.
func (dEnv *DoltEnv) PutDocsToWorking(ctx context.Context, docDetails []doltdb.DocDetails) error {
	wrkRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}
	rootWithDocs, err := dEnv.GetUpdatedRootWithDocs(ctx, wrkRoot, docDetails)
	if err != nil {
		return err
	}
	return dEnv.UpdateWorkingRoot(ctx, rootWithDocs)
}

// PutDocsToStaged adds, updates or removes the `dolt_docs` table on the staged root using the provided docDetails.
func (dEnv *DoltEnv) PutDocsToStaged(ctx context.Context, docDetails []doltdb.DocDetails) (*doltdb.RootValue, error) {
	stgRoot, err := dEnv.StagedRoot(ctx)

	if err != nil {
		return nil, err
	}
	rootWithDocs, err := dEnv.GetUpdatedRootWithDocs(ctx, stgRoot, docDetails)
	if err != nil {
		return nil, err
	}
	_, err = dEnv.UpdateStagedRoot(ctx, rootWithDocs)
	if err != nil {
		return nil, err
	}

	return createDocsTableOnRoot(ctx, dEnv, rootWithDocs, docDetails)
}

func getDocDetails(dEnv *DoltEnv, docDetails []doltdb.DocDetails) ([]doltdb.DocDetails, error) {
	if docDetails == nil {
		docs, err := dEnv.GetAllValidDocDetails()
		if err != nil {
			return nil, err
		}
		return docs, nil
	}
	return docDetails, nil
}

// ResetWorkingDocsToStagedDocs resets the `dolt_docs` table on the working root to match the staged root.
// If the `dolt_docs` table does not exist on the staged root, it will be removed from the working root.
func (dEnv *DoltEnv) ResetWorkingDocsToStagedDocs(ctx context.Context) error {
	wrkRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}

	stgRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return err
	}

	stgDocTbl, stgDocsFound, err := stgRoot.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return err
	}

	_, wrkDocsFound, err := wrkRoot.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return err
	}

	if wrkDocsFound && !stgDocsFound {
		newWrkRoot, err := wrkRoot.RemoveTables(ctx, doltdb.DocTableName)
		if err != nil {
			return err
		}
		return dEnv.UpdateWorkingRoot(ctx, newWrkRoot)
	}

	if stgDocsFound {
		newWrkRoot, err := wrkRoot.PutTable(ctx, doltdb.DocTableName, stgDocTbl)
		if err != nil {
			return err
		}
		return dEnv.UpdateWorkingRoot(ctx, newWrkRoot)
	}
	return nil
}

func updateDocsOnRoot(ctx context.Context, dEnv *DoltEnv, root *doltdb.RootValue, docTbl *doltdb.Table, docDetails []doltdb.DocDetails) (*doltdb.RootValue, error) {
	m, err := docTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := docTbl.GetSchema(context.Background())
	if err != nil {
		return nil, err
	}

	me := m.Edit()
	for _, doc := range docDetails {
		docRow, exists, err := docTbl.GetRowByPKVals(context.Background(), row.TaggedValues{doltdb.DocNameTag: types.String(doc.DocPk)}, sch)
		if err != nil {
			return nil, err
		}

		if exists && doc.NewerText == nil {
			me = me.Remove(docRow.NomsMapKey(sch))
		} else if doc.NewerText != nil {
			docTaggedVals := row.TaggedValues{
				doltdb.DocNameTag: types.String(doc.DocPk),
				doltdb.DocTextTag: types.String(doc.NewerText),
			}
			docRow, err = row.New(types.Format_7_18, sch, docTaggedVals)
			if err != nil {
				return nil, err
			}
			me = me.Set(docRow.NomsMapKey(sch), docRow.NomsMapValue(sch))
		}
	}
	updatedMap, err := me.Map(ctx)
	if updatedMap.Len() == 0 {
		return root.RemoveTables(ctx, doltdb.DocTableName)
	}
	docTbl, err = docTbl.UpdateRows(ctx, updatedMap)
	if err != nil {
		return nil, err
	}
	return root.PutTable(ctx, doltdb.DocTableName, docTbl)
}

func createDocsTableOnRoot(ctx context.Context, dEnv *DoltEnv, root *doltdb.RootValue, docDetails []doltdb.DocDetails) (*doltdb.RootValue, error) {
	typedColColl, _ := schema.NewColCollection(
		schema.NewColumn(doltdb.DocPkColumnName, doltdb.DocNameTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn(doltdb.DocTextColumnName, doltdb.DocTextTag, types.StringKind, false),
	)
	sch := schema.SchemaFromCols(typedColColl)
	imt := table.NewInMemTable(sch)

	createTable := false
	for _, doc := range docDetails {
		if doc.NewerText != nil {
			createTable = true
			docTaggedVals := row.TaggedValues{
				doltdb.DocNameTag: types.String(doc.DocPk),
				doltdb.DocTextTag: types.String(doc.NewerText),
			}

			docRow, err := row.New(types.Format_7_18, sch, docTaggedVals)
			if err != nil {
				return nil, err
			}
			err = imt.AppendRow(docRow)
			if err != nil {
				return nil, err
			}
		}
	}

	if createTable {
		rd := table.NewInMemTableReader(imt)
		wr := noms.NewNomsMapCreator(context.Background(), dEnv.DoltDB.ValueReadWriter(), sch)

		_, _, err := table.PipeRows(context.Background(), rd, wr, false)
		if err != nil {
			return nil, err
		}
		rd.Close(context.Background())
		wr.Close(context.Background())

		vrw := root.VRW()
		schVal, err := encoding.MarshalAsNomsValue(ctx, vrw, wr.GetSchema())

		if err != nil {
			return nil, ErrMarshallingSchema
		}

		newDocsTbl, err := doltdb.NewTable(ctx, root.VRW(), schVal, *wr.GetMap())
		if err != nil {
			return nil, err
		}

		return root.PutTable(ctx, doltdb.DocTableName, newDocsTbl)
	}

	return root, nil
}

//UpdateFSDocsToRootDocs updates the provided docs from the root value, and then saves them to the filesystem.
// If docs == nil, all valid docs will be retrieved and written.
func (dEnv *DoltEnv) UpdateFSDocsToRootDocs(ctx context.Context, root *doltdb.RootValue, docs Docs) error {
	docs, err := dEnv.GetDocsWithNewerTextFromRoot(ctx, root, docs)
	if err != nil {
		return nil
	}
	return docs.Save(dEnv.FS)
}

// GetDocsWithNewerTextFromRoot returns Docs with the NewerText value(s) from the provided root. If docs are provided,
// only those docs will be retrieved and returned. Otherwise, all valid doc details are returned with the updated NewerText.
func (dEnv *DoltEnv) GetDocsWithNewerTextFromRoot(ctx context.Context, root *doltdb.RootValue, docs Docs) (Docs, error) {
	docTbl, docTblFound, err := root.GetTable(ctx, doltdb.DocTableName)
	if err != nil {
		return nil, err
	}

	var sch schema.Schema
	if docTblFound {
		docSch, err := docTbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		sch = docSch
	}

	if docs == nil {
		docs = *AllValidDocDetails
	}

	for i, doc := range docs {
		doc, err = doltdb.AddNewerTextToDocFromTbl(ctx, docTbl, &sch, doc)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}
	return docs, nil
}
