package config

import (
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/filesys"
)

const (
	cfgPath = "/home/bheni/.ld/config.json"
)

func TestGetAndSet(t *testing.T) {
	fs := filesys.NewInMemFS([]string{}, map[string][]byte{}, "/")
	cfg, err := NewFileConfig(cfgPath, fs, map[string]string{})

	if err != nil {
		t.Fatal("Failed to create empty config")
	}

	params := map[string]string{
		"string": "this is a string",
		"int":    "-15",
		"uint":   "1234567",
		"float":  "3.1415",
	}

	err = cfg.SetStrings(params)

	if err != nil {
		t.Fatal("Failed to set values")
	}

	if exists, isDir := fs.Exists(cfgPath); !exists || isDir {
		t.Fatal("File not written after SetStrings was called")
	}

	cfg, err = FromFile(cfgPath, fs)

	if err != nil {
		t.Fatal("Error reading config")
	}

	if str, err := cfg.GetString("string"); err != nil || str != "this is a string" {
		t.Error("Failed to read back string after setting it")
	}

	testIteration(t, params, cfg)

	err = cfg.Unset([]string{"int", "float"})

	if err != nil {
		t.Error("Failed to unset properties")
	}

	testIteration(t, map[string]string{"string": "this is a string", "uint": "1234567"}, cfg)
}
