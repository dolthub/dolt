package verbose

import (
	"context"
	"testing"
)

func TestVerbose(t *testing.T) {
	Logger(context.Background()).Sugar().Warn("This is a test")
	Logger(context.Background()).Sugar().Debug("This is a test with verbse = false")
	SetVerbose(true)
	Logger(context.Background()).Sugar().Debug("This is a test with verbose = true")
}
