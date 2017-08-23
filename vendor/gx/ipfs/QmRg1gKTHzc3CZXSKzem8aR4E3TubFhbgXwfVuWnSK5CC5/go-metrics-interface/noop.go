package metrics

// Also implements the Counter interface
type noop struct{}

func (g *noop) Set(v float64) {
	// Noop
}

func (g *noop) Inc() {
	// Noop
}

func (g *noop) Dec() {
	// Noop
}

func (g *noop) Add(v float64) {
	// Noop
}

func (g *noop) Sub(v float64) {
	// Noop
}

func (g *noop) Observe(v float64) {
	// Noop
}

// Creator functions

func (g *noop) Counter() Counter {
	return g
}

func (g *noop) Gauge() Gauge {
	return g
}

func (g *noop) Histogram(buckets []float64) Histogram {
	return g
}

func (g *noop) Summary(opts SummaryOpts) Summary {
	return g
}
