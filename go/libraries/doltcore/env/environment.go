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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
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

// DoltEnv holds the state of the current environment used by the cli.
type DoltEnv struct {
	Config     *DoltCliConfig
	CfgLoadErr error

	RepoState *RepoState
	RSLoadErr error

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
	ddb, dbLoadErr := doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr)

	dEnv := &DoltEnv{
		config,
		cfgErr,
		repoState,
		rsErr,
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

// InitRepo takes an empty directory and initializes it with a .dolt directory containing repo state, and creates a noms
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
		err = dEnv.initDBAndStateWithTime(ctx, nbf, name, email, t)
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

func (dEnv *DoltEnv) initDBAndState(ctx context.Context, nbf *types.NomsBinFormat, name, email string) error {
	return dEnv.initDBAndStateWithTime(ctx, nbf, name, email, doltdb.CommitNowFunc())
}

func (dEnv *DoltEnv) initDBAndStateWithTime(ctx context.Context, nbf *types.NomsBinFormat, name, email string, t time.Time) error {
	var err error
	dEnv.DoltDB, err = doltdb.LoadDoltDB(ctx, nbf, dEnv.urlStr)

	if err != nil {
		return err
	}

	err = dEnv.DoltDB.WriteEmptyRepoWithCommitTime(ctx, name, email, t)

	if err != nil {
		return doltdb.ErrNomsIO
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := dEnv.DoltDB.Resolve(ctx, cs)

	root, err := commit.GetRootValue()

	if err != nil {
		return err
	}

	rootHash, err := root.HashOf()

	if err != nil {
		return err
	}

	dEnv.RepoState, err = CreateRepoState(dEnv.FS, "master", rootHash)

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (dEnv *DoltEnv) WorkingRoot(ctx context.Context) (*doltdb.RootValue, error) {
	hashStr := dEnv.RepoState.Working
	h := hash.Parse(hashStr)

	return dEnv.DoltDB.ReadRootValue(ctx, h)
}

func (dEnv *DoltEnv) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	h, err := dEnv.DoltDB.WriteRootValue(ctx, newRoot)

	if err != nil {
		return doltdb.ErrNomsIO
	}

	dEnv.RepoState.Working = h.String()
	err = dEnv.RepoState.Save()

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
	hashStr := dEnv.RepoState.Staged
	h := hash.Parse(hashStr)

	return dEnv.DoltDB.ReadRootValue(ctx, h)
}

func (dEnv *DoltEnv) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) (hash.Hash, error) {
	h, err := dEnv.DoltDB.WriteRootValue(ctx, newRoot)

	if err != nil {
		return hash.Hash{}, doltdb.ErrNomsIO
	}

	dEnv.RepoState.Staged = h.String()
	err = dEnv.RepoState.Save()

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

func (dEnv *DoltEnv) getRPCCreds() (credentials.PerRPCCredentials, error) {
	kid, err := dEnv.Config.GetString(UserCreds)

	if err == nil && kid != "" {
		dir, err := dEnv.CredsDir()

		if err != nil {
			// not sure why you wouldn't be able to get the creds dir.
			panic(err)
		}

		dCreds, err := creds.JWKCredsReadFromFile(dEnv.FS, filepath.Join(dir, kid+".jwk"))

		if err != nil {
			return nil, ErrInvalidCredsFile
		}

		return dCreds, nil
	}

	return nil, nil
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
