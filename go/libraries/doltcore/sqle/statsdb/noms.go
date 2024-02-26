package statsdb

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/go-mysql-server/sql"
	"os"
	"path/filepath"
)

type NomsStatsFactory struct{}

var _ StatsFactory = NomsStatsFactory{}

func (sf NomsStatsFactory) Init(fs filesys.Filesys) (Database, error) {
	absPath, err := fs.Abs(dbfactory.DoltStatsDir)
	if err != nil {
		return nil, err
	}

	urlStr := earl.FileUrlFromPath(filepath.ToSlash(absPath), os.PathSeparator)
	urlObj, err := earl.Parse(urlStr)

	exists, isDir := fs.Exists(dbfactory.DoltStatsDir)
	if !exists {
		// create it
		dbfactory.DBFactories[urlObj.Scheme].CreateDB()
	} else if !isDir {
		return nil, fmt.Errorf("file exists where the dolt stats directory should be")
	}

	if urlObj.Scheme
}

/*

stats 
*/
type NomsStats struct {
	
}

var _ Database = (*NomsStats)(nil)

func (n NomsStats) ListStatQuals() []sql.StatQualifier {
	//TODO implement me
	panic("implement me")
}

func (n NomsStats) Load() error {
	//TODO implement me
	panic("implement me")
}

func (n NomsStats) HasStat() {
	//TODO implement me
	panic("implement me")
}

func (n NomsStats) GetStat() {
	//TODO implement me
	panic("implement me")
}

func (n NomsStats) GetAllStats() {
	//TODO implement me
	panic("implement me")
}

func (n NomsStats) PutChunk() {
	//TODO implement me
	panic("implement me")
}

func (n NomsStats) DeleteChunk() {
	//TODO implement me
	panic("implement me")
}
