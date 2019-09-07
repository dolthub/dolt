package commands

import (
	"context"
	"time"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
)

// SendMetricsCommand is the command used for sending metrics
const SendMetricsCommand = "send-metrics"

// var errMetricsDisabled = errors.New("metrics are currently disabled")

// SendMetrics is the commandFunc used that flushes the events to the grpc service
func SendMetrics(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	disabled, err := events.AreMetricsDisabled(dEnv)

	if err != nil {
		// log.Print(err)
		return 1
	}

	if disabled {
		return 0
	}

	if !disabled && err == nil {
		// parse args
		// if it's the output flag
		// use the a WriterFlusher
		// then call the rest of the shit

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		root, err := dEnv.GetUserHomeDir()
		if err != nil {
			// log.Print(err)
			return 1
		}

		dolt := dbfactory.DoltDir

		gef := events.NewGrpcEventFlusher(dEnv.FS, root, dolt, dEnv)

		err = gef.Flush(ctx)

		if err != nil {
			if err == events.ErrFileLocked {
				return 2
			}

			return 1
		}

		return 0
	}

	return 0
}
