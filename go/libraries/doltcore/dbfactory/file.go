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

package dbfactory

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func init() {
	// default to chunk journal unless feature flag is set
	if os.Getenv(dconfig.EnvDisableChunkJournal) != "" {
		chunkJournalFeatureFlag = false
	}
}

var chunkJournalFeatureFlag = true

const (
	// DoltDir defines the directory used to hold the dolt repo data within the filesys
	DoltDir = ".dolt"

	// DataDir is the directory internal to the DoltDir which holds the noms files.
	DataDir = "noms"

	// StatsDir is the directory in DoltDir that holds the database statistics
	StatsDir = "stats"

	ChunkJournalParam = "journal"

	DatabaseNameParam = "database_name"

	MMapArchiveIndexesParam = "mmap_archive_indexes"

	// DisableSingletonCacheParam disables the in-process singleton database cache for local file-backed databases.
	// When set, each open will construct a fresh underlying store instead of reusing a cached instance.
	//
	// Intended for embedded-driver usage where callers want deterministic reopen semantics.
	DisableSingletonCacheParam = "disable_singleton_cache"

	// FailOnJournalLockTimeoutParam changes the journaling store open behavior to fail fast when the exclusive
	// journal manifest lock cannot be acquired within Dolt's internal lock timeout, instead of falling back
	// to opening the database in read-only mode.
	//
	// Intended for embedded-driver usage so higher layers can implement their own retry/backoff policy.
	FailOnJournalLockTimeoutParam = "fail_on_journal_lock_timeout"

	// OpenReadOnlyParam opens the journaling store in read-only mode without attempting to acquire (or hold)
	// the exclusive journal manifest lock. This allows a read-only open to proceed concurrently with another
	// process holding the lock, and ensures the read-only open does not block a subsequent writer.
	//
	// This parameter is only applicable when using the chunk journal (ChunkJournalParam).
	OpenReadOnlyParam = "open_read_only"
)

// DoltDataDir is the directory where noms files will be stored
var DoltDataDir = filepath.Join(DoltDir, DataDir)
var DoltStatsDir = filepath.Join(DoltDir, StatsDir)

// FileFactory is a DBFactory implementation for creating local filesys backed databases
type FileFactory struct {
}

type singletonDB struct {
	ddb datas.Database
	vrw types.ValueReadWriter
	ns  tree.NodeStore
}

var singletonLock = new(sync.Mutex)
var singletons = make(map[string]singletonDB)

func CloseAllLocalDatabases() (err error) {
	singletonLock.Lock()
	defer singletonLock.Unlock()
	for name, s := range singletons {
		if cerr := s.ddb.Close(); cerr != nil {
			err = fmt.Errorf("error closing DB %s (%s)", name, cerr)
		}
	}
	return
}

func DeleteFromSingletonCache(path string, closeIt bool) error {
	singletonLock.Lock()
	defer singletonLock.Unlock()
	var err error
	if closeIt {
		if s, ok := singletons[path]; ok {
			err = s.ddb.Close()
		}
	}
	delete(singletons, path)
	return err
}

// PrepareDB creates the directory for the DB if it doesn't exist, and returns an error if a file or symlink is at the
// path given
func (fact FileFactory) PrepareDB(ctx context.Context, nbf *types.NomsBinFormat, u *url.URL, params map[string]interface{}) error {
	path, err := url.PathUnescape(u.Path)
	if err != nil {
		return err
	}

	path = filepath.FromSlash(path)
	path = u.Host + path

	info, err := os.Stat(path)

	if os.IsNotExist(err) {
		return os.MkdirAll(path, os.ModePerm)
	}

	if err != nil {
		return err
	} else if !info.IsDir() {
		return filesys.ErrIsFile
	}

	return nil
}

// CreateDB creates a local filesys backed database
func (fact FileFactory) CreateDB(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	// Some embedded-driver use-cases require deterministic reopen semantics. When this flag is set,
	// bypass the in-process singleton cache and always create a new underlying store.
	if params != nil {
		if _, ok := params[DisableSingletonCacheParam]; ok {
			return fact.CreateDbNoCache(ctx, nbf, urlObj, params, nbs.JournalParserLoggingWarningsCb)
		}
	}

	singletonLock.Lock()
	defer singletonLock.Unlock()

	if s, ok := singletons[urlObj.Path]; ok {
		return s.ddb, s.vrw, s.ns, nil
	}

	ddb, vrw, ns, err := fact.CreateDbNoCache(ctx, nbf, urlObj, params, nbs.JournalParserLoggingWarningsCb)
	if err != nil {
		return nil, nil, nil, err
	}

	singletons[urlObj.Path] = singletonDB{
		ddb: ddb,
		vrw: vrw,
		ns:  ns,
	}

	return ddb, vrw, ns, nil
}

// CreateDbNoCache creates a local filesys backed database without using the singleton cache. This is used for a very specific
// case: the `dolt fsck` command. Since database loading happens before subcommand execution, and `dolt fsck` needs to report
// journal issues, it needs to load the database without simply printing an error to the log for journal issues.
//
// Furthermore, regular database loading uses this code path to construct the GenerationalCS, which is desired because we
// want the same underlying implementation.
func (fact FileFactory) CreateDbNoCache(ctx context.Context, nbf *types.NomsBinFormat, urlObj *url.URL, params map[string]interface{}, recCb func(error)) (datas.Database, types.ValueReadWriter, tree.NodeStore, error) {
	path, err := url.PathUnescape(urlObj.Path)
	if err != nil {
		return nil, nil, nil, err
	}

	path = filepath.FromSlash(path)
	path = urlObj.Host + path

	err = validateDir(path)
	if err != nil {
		return nil, nil, nil, err
	}

	var useJournal bool
	var mmapArchiveIndexes bool
	if params != nil {
		_, useJournal = params[ChunkJournalParam]
		_, mmapArchiveIndexes = params[MMapArchiveIndexesParam]
	}

	var newGenSt *nbs.NomsBlockStore
	q := nbs.NewUnlimitedMemQuotaProvider()
	if useJournal && chunkJournalFeatureFlag {
		// Allow higher layers (e.g. embedded driver) to opt into fail-fast lock behavior instead of
		// falling back to read-only mode on lock timeout.
		opts := nbs.JournalingStoreOptions{}
		if params != nil {
			if _, ok := params[FailOnJournalLockTimeoutParam]; ok {
				opts.FailOnLockTimeout = true
			}
			if _, ok := params[OpenReadOnlyParam]; ok {
				opts.ReadOnly = true
			}
		}
		newGenSt, err = nbs.NewLocalJournalingStoreWithOptions(ctx, nbf.VersionString(), path, q, mmapArchiveIndexes, recCb, opts)
	} else {
		newGenSt, err = nbs.NewLocalStore(ctx, nbf.VersionString(), path, defaultMemTableSize, q, mmapArchiveIndexes)
	}

	if err != nil {
		return nil, nil, nil, err
	}

	oldgenPath := filepath.Join(path, "oldgen")
	err = validateDir(oldgenPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil, err
		}

		err = os.Mkdir(oldgenPath, os.ModePerm)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return nil, nil, nil, err
		}
	}

	oldGenSt, err := nbs.NewLocalStore(ctx, newGenSt.Version(), oldgenPath, defaultMemTableSize, q, mmapArchiveIndexes)
	if err != nil {
		return nil, nil, nil, err
	}

	ghostGen, err := nbs.NewGhostBlockStore(path)
	if err != nil {
		return nil, nil, nil, err
	}

	st := nbs.NewGenerationalCS(oldGenSt, newGenSt, ghostGen)
	// metrics?

	if params != nil {
		if nameV, ok := params[DatabaseNameParam]; ok && nameV != nil {
			if name, ok := nameV.(string); ok && name != "" {
				st.AppendLoggerFields(logrus.Fields{"database": name})
			}
		}
	}

	vrw := types.NewValueStore(st)
	ns := tree.NewNodeStore(st)
	ddb := datas.NewTypesDatabase(vrw, ns)

	return ddb, vrw, ns, nil
}

func validateDir(path string) error {
	info, err := os.Stat(path)

	if err != nil {
		return err
	} else if !info.IsDir() {
		return filesys.ErrIsFile
	}

	return nil
}
