package env

import "testing"

const (
	email = "bigbillieb@fake.horse"
	name  = "Billy Bob"
)

func TestConfig(t *testing.T) {
	dEnv := createTestEnv(true, true)

	lCfg, _ := dEnv.Config.GetConfig(LocalConfig)
	gCfg, _ := dEnv.Config.GetConfig(GlobalConfig)

	lCfg.SetStrings(map[string]string{UserEmailKey: email})
	gCfg.SetStrings(map[string]string{UserNameKey: name})

	if *dEnv.Config.GetStringOrDefault(UserEmailKey, "no") != email {
		t.Error("Should return", email)
	}

	if *dEnv.Config.GetStringOrDefault("bad_key", "yes") != "yes" {
		t.Error("Should return default value of yes")
	}

	if dEnv.Config.IfEmptyUseConfig("", UserEmailKey) != email {
		t.Error("Should return", email)
	}

	if dEnv.Config.IfEmptyUseConfig("not empty", UserEmailKey) != "not empty" {
		t.Error("Should return default value")
	}

	if dEnv.Config.IfEmptyUseConfig("", "missing") != "" {
		t.Error("Should return empty string")
	}
}
