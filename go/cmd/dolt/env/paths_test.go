package env

import (
	"path/filepath"
	"testing"
)

func TestGetGlobalCfgPath(t *testing.T) {
	homeDir := "/user/bheni"
	expected := filepath.Join(homeDir, DoltDir, globalConfig)
	actual, _ := getGlobalCfgPath(func() (string, error) {
		return homeDir, nil
	})

	if actual != expected {
		t.Error(actual, "!=", expected)
	}
}
