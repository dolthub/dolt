package metrics

var _ Counter = (*noop)(nil)
var _ Gauge = (*noop)(nil)
var _ Histogram = (*noop)(nil)
var _ Summary = (*noop)(nil)

var _ Creator = (*noop)(nil)
