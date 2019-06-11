package env

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"path/filepath"
	"testing"
)

func TestGetGlobalCfgPath(t *testing.T) {
	homeDir := "/user/bheni"
	expected := filepath.Join(homeDir, dbfactory.DoltDir, globalConfig)
	actual, _ := getGlobalCfgPath(func() (string, error) {
		return homeDir, nil
	})

	if actual != expected {
		t.Error(actual, "!=", expected)
	}
}
