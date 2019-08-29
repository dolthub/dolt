package commands

import (
	"context"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

// SendMetricsCommand is the command used for sending metrics
const SendMetricsCommand = "send-metrics"

// var errMetricsDisabled = errors.New("metrics are currently disabled")

// SendMetrics is the commandFunc used that flushes the events to the grpc service
func SendMetrics(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	disabled, err := events.AreMetricsDisabled(dEnv)
	if !disabled && err == nil {
		fs := filesys.LocalFS

		root, err := env.GetCurrentUserHomeDir()
		if err != nil {
			// log.Print(err)
			return 1
		}

		dolt := dbfactory.DoltDir

		eventGrpcFlush := events.NewEventGrpcFlush(fs, root, dolt, dEnv)

		if err := eventGrpcFlush.FlushEvents(); err != nil {
			// log.Print(err)
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
