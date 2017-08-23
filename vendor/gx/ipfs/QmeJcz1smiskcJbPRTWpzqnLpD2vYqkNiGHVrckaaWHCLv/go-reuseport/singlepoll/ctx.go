package singlepoll

import "context"

var (
	backgroundctx    context.Context
	backgroundcancel context.CancelFunc
)

func init() {
	backgroundctx, backgroundcancel = context.WithCancel(context.Background())
}

func CloseBackgroundProcesses() {
	backgroundcancel()
}
