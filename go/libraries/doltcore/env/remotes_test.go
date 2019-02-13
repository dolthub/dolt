package env

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"reflect"
	"testing"
)

func Test_parseRemotesFromConfig(t *testing.T) {
	in := map[string]string{
		"user.name":         "bheni",
		"remote.local.url":  "http://localhost:50051/org/repo",
		"remote.origin.url": "https://dolthub.com/org/repo",
	}
	expectedResults := map[string]*Remote{
		"local":  {Name: "local", Url: "http://localhost:50051/org/repo"},
		"origin": {Name: "origin", Url: "https://dolthub.com/org/repo"},
	}

	cfg := config.NewMapConfig(in)
	results, err := parseRemotesFromConfig(cfg)

	if err != nil {
		t.Error(err.Error())
	}

	if len(results) != len(expectedResults) {
		t.Fatal("Unexpected result length")
	}

	for k, val := range results {
		expectedVal := expectedResults[k]

		if !reflect.DeepEqual(*val, *expectedVal) {
			t.Error(*val, "!=", *expectedVal)
		}
	}
}
