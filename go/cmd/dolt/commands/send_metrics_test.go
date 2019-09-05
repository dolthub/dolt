package commands

import (
	"context"
	"path/filepath"
	"testing"

	// "github.com/liquidata-inc/dolt/go/cmd/dolt/commands"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/assert"
)

var (
	smHomeDir     = "/home/"
	doltDir       = filepath.Join(smHomeDir, ".dolt")
	eventsDataDir = filepath.Join(doltDir, "eventsData")
)

func testSMHomeDirFunc() (string, error) {
	return smHomeDir, nil
}

func createSendMetricsTestEnv() *env.DoltEnv {
	fs := filesys.NewInMemFS([]string{eventsDataDir}, nil, eventsDataDir)

	dEnv := env.Load(context.Background(), testSMHomeDirFunc, fs, doltdb.InMemDoltDB)

	return dEnv
}

func TestFlockAndFlush(t *testing.T) {
	t.Run("test flock and flush func", func(t *testing.T) {
		dEnv := createSendMetricsTestEnv()

		exists, _ := dEnv.FS.Exists(eventsDataDir)
		assert.Equal(t, exists, true)

		// test with deadline?
		ctx := context.Background()

		// create test client
		client := events.NewTestClient()

		// create fbp
		sn := events.NewSequentialNamer()
		fbp := events.NewFileBackedProc(dEnv.FS, dEnv.GetUserHomeDir(), doltDir, sn.Name, sn.Check)

		// create egf
		em := &events.GrpcEmitter{client}
		egf := &events.EventGrpcFlush{em: em, fbp: fbp, LockPath: fbp.GetEventsDirPath()}

		// start two processes
		// call flock and flush in each process

		result := make(chan error)

		go func() {
			err = flockAndFlush(ctx, dEnv, egf)
			// send err to err chan
		}()
		go func() {
			err = flockAndFlush(ctx, dEnv, egf)
			// send err to err chan
		}()

		// assert the outputs from each process are zero, should not get number errs even if dir is locked
		// assert that the clients ces only has the correct number of events and not double
	})
}
