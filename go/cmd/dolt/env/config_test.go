package env

import "testing"

const (
	email = "bigbillieb@fake.horse"
	name  = "Billy Bob"
)

func TestConfig(t *testing.T) {
	cliEnv := createTestEnv(true, true)

	lCfg, _ := cliEnv.Config.GetConfig(LocalConfig)
	gCfg, _ := cliEnv.Config.GetConfig(GlobalConfig)

	lCfg.SetStrings(map[string]string{UserEmailKey: email})
	gCfg.SetStrings(map[string]string{UserNameKey: name})

	if *cliEnv.Config.GetStringOrDefault(UserEmailKey, "no") != email {
		t.Error("Should return", email)
	}

	if *cliEnv.Config.GetStringOrDefault("bad_key", "yes") != "yes" {
		t.Error("Should return default value of yes")
	}

	if *cliEnv.Config.IfEmptyUseConfig("", UserEmailKey) != email {
		t.Error("Should return", email)
	}

	if *cliEnv.Config.IfEmptyUseConfig("not empty", UserEmailKey) != "not empty" {
		t.Error("Should return default value")
	}

	if *cliEnv.Config.IfEmptyUseConfig("", "missing") != "" {
		t.Error("Should return empty string")
	}
}
