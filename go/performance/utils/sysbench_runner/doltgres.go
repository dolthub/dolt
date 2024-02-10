package sysbench_runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const (
	postgresDriver         = "postgres"
	doltgresUser           = "doltgres"
	doltDataDir            = ".dolt"
	createDatabaseTemplate = "create database %s;"
	psqlDsnTemplate        = "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable"
)

type doltgresBenchmarkerImpl struct {
	dir          string // cwd
	config       *Config
	serverConfig *ServerConfig
}

var _ Benchmarker = &doltgresBenchmarkerImpl{}

func NewDoltgresBenchmarker(dir string, config *Config, serverConfig *ServerConfig) *doltgresBenchmarkerImpl {
	return &doltgresBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *doltgresBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.ServerExec, doltVersionCommand)
	return version.Run()
}

func (b *doltgresBenchmarkerImpl) createServerDir() (string, error) {
	return CreateServerDir(dbName)
}

func (b *doltgresBenchmarkerImpl) cleanupServerDir(dir string) error {
	dataDir := filepath.Join(dir, doltDataDir)
	defaultDir := filepath.Join(dir, doltgresUser)
	testDir := filepath.Join(dir, dbName)
	for _, d := range []string{dataDir, defaultDir, testDir} {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			err = os.RemoveAll(d)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *doltgresBenchmarkerImpl) createTestingDb(ctx context.Context) error {
	psqlconn := fmt.Sprintf(psqlDsnTemplate, b.serverConfig.Host, b.serverConfig.Port, doltgresUser, "", dbName)

	// open database
	db, err := sql.Open(postgresDriver, psqlconn)
	if err != nil {
		return err
	}

	// close database
	defer db.Close()

	// check db
	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(createDatabaseTemplate, dbName))
	return err
}

func (b *doltgresBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	err := b.checkInstallation(ctx)
	if err != nil {
		return nil, err
	}

	serverDir, err := CreateServerDir(dbName)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanupDoltgresServerDir(serverDir)
	}()

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}
	serverParams = append(serverParams, fmt.Sprintf("%s=%s", doltgresDataDirFlag, serverDir))

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start(ctx)
	if err != nil {
		return nil, err
	}

	err = b.createTestingDb(ctx)
	if err != nil {
		return nil, err
	}

	tests, err := GetTests(b.config, b.serverConfig, nil)
	if err != nil {
		return nil, err
	}

	results := make(Results, 0)
	for i := 0; i < b.config.Runs; i++ {
		for _, test := range tests {
			tester := NewSysbenchTester(b.config, b.serverConfig, test, stampFunc)
			r, err := tester.Test(ctx)
			if err != nil {
				server.Stop(ctx)
				return nil, err
			}
			results = append(results, r)
		}
	}

	err = server.Stop(ctx)
	if err != nil {
		return nil, err
	}

	return results, nil
}
