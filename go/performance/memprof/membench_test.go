package memprof

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var loc = flag.String("doltDir", "", "Directory of dolt database")
var urlStr string
var ddb *doltdb.DoltDB

func TestMain(m *testing.M) {
	flag.Parse()
	if *loc == "" {
		log.Panicf("doltDir must be specified")
	}

	urlStr = "file://" + *loc + dbfactory.DoltDataDir

	code := m.Run()
	os.Exit(code)
}

func BenchmarkLoadDoltDBMemory(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		var err error
		ddb, err = doltdb.LoadDoltDB(ctx, types.Format_Default, urlStr, filesys.LocalFS)
		if err != nil {
			b.Fatalf("failed to load doltdb, err: %s", err.Error())
		}
	}
}
