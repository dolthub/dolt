package sqle

import (
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/src-d/go-mysql-server/sql"
)

type dbRoot struct {
	hashStr string
	root *doltdb.RootValue
}

type DoltSession struct {
	sql.Session
	dbRoots map[string]dbRoot
}

func DefaultDoltSession() *DoltSession {
	return &DoltSession{sql.NewBaseSession(), make(map[string]dbRoot)}
}

func NewSessionWithDefaultRoots(sqlSess sql.Session, dbs ...Database) (*DoltSession, error) {
	dbRoots := make(map[string]dbRoot)
	for _, db := range dbs {
		defRoot := db.GetDefaultRoot()
		h, err := defRoot.HashOf()

		if err != nil {
			return nil, err
		}

		hashStr := h.String()

		dbRoots[db.Name()] = dbRoot{hashStr:hashStr, root:defRoot}
	}

	return &DoltSession{sqlSess, dbRoots}, nil
}

func DSessFromSess(sess sql.Session) *DoltSession {
	return sess.(*DoltSession)
}