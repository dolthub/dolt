package env

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
)

const (
	// The directory where configuration and state information will be written within a data repo directory
	DoltDir = ".dolt"
)

// DoltCLIEnv holds the state of the current environment used by the cli.
type DoltCLIEnv struct {
	Config     *DoltCliConfig
	CfgLoadErr error

	RepoState *RepoState
	RSLoadErr error

	DoltDB *doltdb.DoltDB

	FS filesys.Filesys
}

// Load loads the DoltCLIEnv for the current directory of the cli
func Load(hdp HomeDirProvider, fs filesys.Filesys, loc doltdb.DoltDBLocation) *DoltCLIEnv {
	config, cfgErr := loadDoltCliConfig(hdp, fs)
	repoState, rsErr := LoadRepoState(fs)
	ddb := doltdb.LoadDoltDB(loc)

	return &DoltCLIEnv{
		config,
		cfgErr,
		repoState,
		rsErr,
		ddb,
		fs,
	}
}

// HasLDDir returns true of the DoltDir directory exists and is a valid directory
func (cliEnv *DoltCLIEnv) HasLDDir() bool {
	exists, isDir := cliEnv.FS.Exists(getDoltDir())
	return exists && isDir
}

// IsCWDEmpty returns wheather the current working directory is empty or not.
func (cliEnv *DoltCLIEnv) IsCWDEmpty() bool {
	isEmpty := true
	cliEnv.FS.Iter("./", true, func(_ string, _ int64, _ bool) bool {
		isEmpty = false
		return true
	})

	return isEmpty
}

// HasLocalConfig returns true if a repository local config file
func (cliEnv *DoltCLIEnv) HasLocalConfig() bool {
	_, ok := cliEnv.Config.GetConfig(LocalConfig)

	return ok
}

func (cliEnv *DoltCLIEnv) bestEffortDeleteAllFromCWD() {
	fileToIsDir := make(map[string]bool)
	cliEnv.FS.Iter("./", false, func(path string, size int64, isDir bool) (stop bool) {
		fileToIsDir[path] = isDir
		return false
	})

	for path, isDir := range fileToIsDir {
		if isDir {
			cliEnv.FS.Delete(path, true)
		} else {
			cliEnv.FS.DeleteFile(path)
		}
	}
}

// InitRepo takes an empty directory and initializes it with a .dolt directory containing repo state, and creates a noms
// database with dolt structure.
func (cliEnv *DoltCLIEnv) InitRepo(name, email string) errhand.VerboseError {
	if !cliEnv.IsCWDEmpty() {
		bdr := errhand.BuildDError("Unable to initialize the current directory.")
		bdr.AddDetails("dolt will only allow empty directoriese to be initialized.")
		return bdr.Build()
	}

	err := cliEnv.FS.MkDirs(DoltDir)

	if err != nil {
		bdr := errhand.BuildDError("Unable to make directory %s within the working directory.", DoltDir)
		return bdr.AddCause(err).Build()
	}

	err = cliEnv.Config.CreateLocalConfig(map[string]string{})

	if err != nil {
		cliEnv.bestEffortDeleteAllFromCWD()
		bdr := errhand.BuildDError("Failed to create an empty local data repository configuration file.")
		bdr.AddDetails("Failed creating file %s.", getLocalConfigPath())
		return bdr.AddCause(err).Build()
	}

	err = cliEnv.DoltDB.WriteEmptyRepo(name, email)

	if err != nil {
		cliEnv.bestEffortDeleteAllFromCWD()
		bdr := errhand.BuildDError("Unable to create the local data repository.")
		return bdr.AddCause(err).Build()
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", "master")
	commit, _ := cliEnv.DoltDB.Resolve(cs)

	rootHash := commit.GetRootValue().HashOf()
	cliEnv.RepoState, err = CreateRepoState(cliEnv.FS, "master", rootHash)

	if err != nil {
		cliEnv.bestEffortDeleteAllFromCWD()
		bdr := errhand.BuildDError("Unable to write the initial repository state.")
		bdr.AddDetails("Failed creating file %s.", getRepoStateFile())
		return bdr.AddCause(err).Build()
	}

	return nil
}

func (cliEnv *DoltCLIEnv) WorkingRoot() (*doltdb.RootValue, error) {
	hashStr := cliEnv.RepoState.Working
	h := hash.Parse(hashStr)

	return cliEnv.DoltDB.ReadRootValue(h)
}

func (cliEnv *DoltCLIEnv) UpdateWorkingRoot(newRoot *doltdb.RootValue) errhand.VerboseError {
	h, err := cliEnv.DoltDB.WriteRootValue(newRoot)

	if err != nil {
		bdr := errhand.BuildDError("Unable to write table to the noms DB.")
		return bdr.AddCause(err).Build()
	}

	cliEnv.RepoState.Working = h.String()
	err = cliEnv.RepoState.Save()

	if err != nil {
		bdr := errhand.BuildDError("Unable to save an updated working value to the local data repositories state.")
		return bdr.AddCause(err).Build()
	}

	return nil
}

func (cliEnv *DoltCLIEnv) HeadRoot() (*doltdb.RootValue, error) {
	cs, _ := doltdb.NewCommitSpec("head", cliEnv.RepoState.Branch)
	commit, err := cliEnv.DoltDB.Resolve(cs)

	if err != nil {
		return nil, err
	}

	return commit.GetRootValue(), nil
}

func (cliEnv *DoltCLIEnv) StagedRoot() (*doltdb.RootValue, error) {
	hashStr := cliEnv.RepoState.Staged
	h := hash.Parse(hashStr)

	return cliEnv.DoltDB.ReadRootValue(h)
}

func (cliEnv *DoltCLIEnv) UpdateStagedRoot(newRoot *doltdb.RootValue) errhand.VerboseError {
	h, err := cliEnv.DoltDB.WriteRootValue(newRoot)

	if err != nil {
		bdr := errhand.BuildDError("Unable to write table to the noms DB.")
		return bdr.AddCause(err).Build()
	}

	cliEnv.RepoState.Staged = h.String()
	err = cliEnv.RepoState.Save()

	if err != nil {
		bdr := errhand.BuildDError("Unable to save an updated staged value to the local data repositories state.")
		return bdr.AddCause(err).Build()
	}

	return nil
}

func (cliEnv *DoltCLIEnv) PutTableToWorking(rows types.Map, sch *schema.Schema, tableName string) errhand.VerboseError {
	root, err := cliEnv.WorkingRoot()

	if err != nil {
		bdr := errhand.BuildDError("Unable to get working root.")
		return bdr.AddCause(err).Build()
	}

	vrw := cliEnv.DoltDB.ValueReadWriter()
	schVal, err := noms.MarshalAsNomsValue(vrw, sch)

	if err != nil {
		bdr := errhand.BuildDError("Unable to marshal schema as noms value.")
		return bdr.AddCause(err).Build()
	}

	tbl := doltdb.NewTable(vrw, schVal, rows)
	newRoot := root.PutTable(cliEnv.DoltDB, tableName, tbl)
	return cliEnv.UpdateWorkingRoot(newRoot)
}
