package commands

import (
	"context"

	// "github.com/juju/fslock"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

// SendMetricsCommand is the command used for sending metrics
const SendMetricsCommand = "send-metrics"

// var errMetricsDisabled = errors.New("metrics are currently disabled")

// func fLock(lockFilePath string) (*fslock.Lock, error) {
// 	lck := fslock.New(lockFilePath)
// 	err := lck.Lock()

// 	if err != nil {
// 		return nil, err
// 	}

// 	return lck, nil
// }

// SendMetrics is the commandFunc used that flushes the events to the grpc service
func SendMetrics(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	disabled, err := events.AreMetricsDisabled(dEnv)
	if !disabled && err == nil {
		fs := filesys.LocalFS

		// context.WithDeadline()

		root, err := env.GetCurrentUserHomeDir()
		if err != nil {
			// log.Print(err)
			return 1
		}

		dolt := dbfactory.DoltDir

		egf := events.NewEventGrpcFlush(fs, root, dolt, dEnv)

		// init lock
		lck := fslock.New(egf.LockPath)

		// try the lock
		if err := lck.TryLock(); err != nil {
			// log.Print("Trylock block")
			log.Print(err)
			return 1
		}

		// if no err, lock the lock
		if err := lck.Lock(); err != nil {
			// log.Print("Lock block")
			log.Print(err)
			return 1
		}
	
		// process dir
		if err := egf.FlushEvents(); err != nil {
			log.Print(err)
			return 1
		}

		// unlock dir
		if err := lck.Unlock(); err != nil {
			log.Print(err)
			return 1
		}

		return 0
	}

	if err != nil {
		// log.Print(err)
		return 1
	}

	// log.Print(errMetricsDisabled)
	return 0
}
