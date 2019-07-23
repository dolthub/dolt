package commands

import (
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
)

func TestInit(t *testing.T) {
	tests := []struct {
		Name          string
		Args          []string
		GlobalConfig  map[string]string
		ExpectSuccess bool
	}{
		{
			"Command Line name and email",
			[]string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"},
			map[string]string{},
			true,
		},
		{
			"Global config name and email",
			[]string{},
			map[string]string{
				env.UserNameKey:  "Bill Billerson",
				env.UserEmailKey: "bigbillieb@fake.horse",
			},
			true,
		},
		{
			"No Name",
			[]string{"-email", "bigbillieb@fake.horse"},
			map[string]string{},
			false,
		},
		{
			"No Email",
			[]string{"-name", "Bill Billerson"},
			map[string]string{},
			false,
		},
	}

	for _, test := range tests {
		dEnv := createUninitializedEnv()
		gCfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
		gCfg.SetStrings(test.GlobalConfig)

		result := Init("dolt init", test.Args, dEnv)

		if (result == 0) != test.ExpectSuccess {
			t.Error(test.Name, "- Expected success:", test.ExpectSuccess, "result:", result == 0)
		} else if test.ExpectSuccess {
			// succceeded as expected
			if !dEnv.HasDoltDir() {
				t.Error(test.Name, "- .dolt dir should exist after initialization")
			}
		} else {
			// failed as expected
			if dEnv.HasDoltDir() {
				t.Error(test.Name, "- dolt directory shouldn't exist after failure to initialize")
			}
		}
	}
}

func TestInitTwice(t *testing.T) {
	dEnv := createUninitializedEnv()
	result := Init("dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv)

	if result != 0 {
		t.Error("First init should succeed")
	}

	result = Init("dolt init", []string{"-name", "Bill Billerson", "-email", "bigbillieb@fake.horse"}, dEnv)

	if result == 0 {
		t.Error("Second init should fail")
	}
}
