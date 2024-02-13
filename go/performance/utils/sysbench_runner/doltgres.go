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
	config       SysbenchConfig
	serverConfig ServerConfig
}

var _ Benchmarker = &doltgresBenchmarkerImpl{}

func NewDoltgresBenchmarker(dir string, config SysbenchConfig, serverConfig ServerConfig) *doltgresBenchmarkerImpl {
	return &doltgresBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *doltgresBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.GetServerExec(), doltVersionCommand)
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
	psqlconn := fmt.Sprintf(psqlDsnTemplate, b.serverConfig.GetHost(), b.serverConfig.GetPort(), doltgresUser, "", dbName)

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

func (b *doltgresBenchmarkerImpl) Benchmark(ctx context.Context) (results Results, err error) {
	err = b.checkInstallation(ctx)
	if err != nil {
		return
	}

	var serverDir string
	serverDir, err = CreateServerDir(dbName)
	if err != nil {
		return
	}
	defer func() {
		rerr := b.cleanupServerDir(serverDir)
		if err == nil {
			err = rerr
		}
	}()

	var serverParams []string
	serverParams, err = b.serverConfig.GetServerArgs()
	if err != nil {
		return
	}

	serverParams = append(serverParams, fmt.Sprintf("%s=%s", doltgresDataDirFlag, serverDir))

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start()
	if err != nil {
		return
	}

	err = b.createTestingDb(ctx)
	if err != nil {
		return
	}

	var tests []Test
	tests, err = GetTests(b.config, b.serverConfig)
	if err != nil {
		return
	}

	results = make(Results, 0)
	runs := b.config.GetRuns()
	for i := 0; i < runs; i++ {
		for _, test := range tests {
			tester := NewSysbenchTester(b.config, b.serverConfig, test, serverParams, stampFunc)
			var r *Result
			r, err = tester.Test(ctx)
			if err != nil {
				server.Stop()
				return
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return
	}

	return
}

// CreateServerDir creates a server directory
func CreateServerDir(dbName string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	serverDir := filepath.Join(cwd, dbName)
	err = os.MkdirAll(serverDir, os.ModePerm)
	if err != nil {
		return "", err
	}

	return serverDir, nil
}
