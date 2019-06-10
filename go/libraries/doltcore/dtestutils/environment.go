package dtestutils

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

const (
	TestHomeDir = "/user/bheni"
	WorkingDir  = "/user/bheni/datasets/states"
)

func testHomeDirFunc() (string, error) {
	return TestHomeDir, nil
}

func CreateTestEnv() *env.DoltEnv {
	const name = "billy bob"
	const email = "bigbillieb@fake.horse"
	initialDirs := []string{TestHomeDir, WorkingDir}
	fs := filesys.NewInMemFS(initialDirs, nil, WorkingDir)
	dEnv := env.Load(context.Background(), testHomeDirFunc, fs, doltdb.InMemDoltDB)
	cfg, _ := dEnv.Config.GetConfig(env.GlobalConfig)
	cfg.SetStrings(map[string]string{
		env.UserNameKey:  name,
		env.UserEmailKey: email,
	})
	err := dEnv.InitRepo(context.Background(), name, email)

	if err != nil {
		panic("Failed to initialize environment")
	}

	return dEnv
}
