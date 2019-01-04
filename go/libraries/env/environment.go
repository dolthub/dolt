package env

import (
	"fmt"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"github.com/pkg/errors"
)

const (
	// The directory where configuration and state information will be written within a data repo directory
	DoltDir = ".dolt"
)

var ErrDirNotEmpty = errors.New("directory is not empty")
var ErrNomsIO = errors.New("error reading from or writing to noms")
var ErrStateUpdate = errors.New("error updating local data repo state")
var ErrMarshallingSchema = errors.New("error marshalling schema")

// DoltEnv holds the state of the current environment used by the cli.
type DoltEnv struct {
	Config     *DoltCliConfig
	CfgLoadErr error

	RepoState *RepoState
	RSLoadErr error

	DoltDB *doltdb.DoltDB

	FS filesys.Filesys
}

// Load loads the DoltEnv for the current directory of the cli
func Load(hdp HomeDirProvider, fs filesys.Filesys, loc doltdb.DoltDBLocation) *DoltEnv {
	config, cfgErr := loadDoltCliConfig(hdp, fs)
	repoState, rsErr := LoadRepoState(fs)
	ddb := doltdb.LoadDoltDB(loc)

	return &DoltEnv{
		config,
		cfgErr,
		repoState,
		rsErr,
		ddb,
		fs,
	}
}

// HasLDDir returns true of the DoltDir directory exists and is a valid directory
func (dEnv *DoltEnv) HasLDDir() bool {
	exists, isDir := dEnv.FS.Exists(getDoltDir())
	return exists && isDir
}

// IsCWDEmpty returns wheather the current working directory is empty or not.
func (dEnv *DoltEnv) IsCWDEmpty() bool {
	isEmpty := true
	dEnv.FS.Iter("./", true, func(_ string, _ int64, _ bool) bool {
		isEmpty = false
		return true
	})

	return isEmpty
}

// HasLocalConfig returns true if a repository local config file
func (dEnv *DoltEnv) HasLocalConfig() bool {
	_, ok := dEnv.Config.GetConfig(LocalConfig)

	return ok
}

func (dEnv *DoltEnv) bestEffortDeleteAllFromCWD() {
	fileToIsDir := make(map[string]bool)
	dEnv.FS.Iter("./", false, func(path string, size int64, isDir bool) (stop bool) {
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
func (dEnv *DoltEnv) InitRepo(name, email string) error {
	if !dEnv.IsCWDEmpty() {
		return ErrDirNotEmpty
	}

	err := dEnv.FS.MkDirs(DoltDir)

	if err != nil {
		return fmt.Errorf("unable to make directory %s within the working directory", DoltDir)
	}

	err = dEnv.Config.CreateLocalConfig(map[string]string{})

	if err != nil {
		dEnv.bestEffortDeleteAllFromCWD()
		return fmt.Errorf("failed creating file %s", getLocalConfigPath())
	}

	err = dEnv.DoltDB.WriteEmptyRepo(name, email)

	if err != nil {
		dEnv.bestEffortDeleteAllFromCWD()
		return ErrNomsIO
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := dEnv.DoltDB.Resolve(cs)

	rootHash := commit.GetRootValue().HashOf()
	dEnv.RepoState, err = CreateRepoState(dEnv.FS, "master", rootHash)

	if err != nil {
		dEnv.bestEffortDeleteAllFromCWD()
		return ErrStateUpdate
	}

	return nil
}

func (dEnv *DoltEnv) WorkingRoot() (*doltdb.RootValue, error) {
	hashStr := dEnv.RepoState.Working
	h := hash.Parse(hashStr)

	return dEnv.DoltDB.ReadRootValue(h)
}

func (dEnv *DoltEnv) UpdateWorkingRoot(newRoot *doltdb.RootValue) error {
	h, err := dEnv.DoltDB.WriteRootValue(newRoot)

	if err != nil {
		return ErrNomsIO
	}

	dEnv.RepoState.Working = h.String()
	err = dEnv.RepoState.Save()

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (dEnv *DoltEnv) HeadRoot() (*doltdb.RootValue, error) {
	cs, _ := doltdb.NewCommitSpec("head", dEnv.RepoState.Branch)
	commit, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue(), nil
}

func (dEnv *DoltEnv) StagedRoot() (*doltdb.RootValue, error) {
	hashStr := dEnv.RepoState.Staged
	h := hash.Parse(hashStr)

	return dEnv.DoltDB.ReadRootValue(h)
}

func (dEnv *DoltEnv) UpdateStagedRoot(newRoot *doltdb.RootValue) error {
	h, err := dEnv.DoltDB.WriteRootValue(newRoot)

	if err != nil {
		return ErrNomsIO
	}

	dEnv.RepoState.Staged = h.String()
	err = dEnv.RepoState.Save()

	if err != nil {
		return ErrStateUpdate
	}

	return nil
}

func (dEnv *DoltEnv) PutTableToWorking(rows types.Map, sch *schema.Schema, tableName string) error {
	root, err := dEnv.WorkingRoot()

	if err != nil {
		return ErrNomsIO
	}

	vrw := dEnv.DoltDB.ValueReadWriter()
	schVal, err := noms.MarshalAsNomsValue(vrw, sch)

	if err != nil {
		return ErrMarshallingSchema
	}

	tbl := doltdb.NewTable(vrw, schVal, rows)
	newRoot := root.PutTable(dEnv.DoltDB, tableName, tbl)
	return dEnv.UpdateWorkingRoot(newRoot)
}
