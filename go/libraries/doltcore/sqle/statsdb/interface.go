package statsdb

import (
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/go-mysql-server/sql"
)

// database needs to read and write statistics
// also need to delete/add specific chunks
type Database interface {
	ListStatQuals() []sql.StatQualifier
	Load() error
	HasStat()
	GetStat()
	GetAllStats()
	PutChunk()
	DeleteChunk()
}

type StatsFactory interface {
	Init(fs filesys.Filesys) (Database, error)
}
