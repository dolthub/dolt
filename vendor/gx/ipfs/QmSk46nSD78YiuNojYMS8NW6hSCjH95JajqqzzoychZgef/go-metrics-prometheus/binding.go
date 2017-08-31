package metricsprometheus

import (
	"strings"

	metrics "gx/ipfs/QmRg1gKTHzc3CZXSKzem8aR4E3TubFhbgXwfVuWnSK5CC5/go-metrics-interface"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	pro "gx/ipfs/QmX3QZ5jHEPidwUrymXV1iSCSUhdGxj15sm2gP4jKMef7B/client_golang/prometheus"
)

var log logging.EventLogger = logging.Logger("metrics-prometheus")

func Inject() error {
	return metrics.InjectImpl(newCreator)
}

func newCreator(name, helptext string) metrics.Creator {
	return &creator{
		name:     strings.Replace(name, ".", "_", -1),
		helptext: helptext,
	}
}

var _ metrics.Creator = &creator{}

type creator struct {
	name     string
	helptext string
}

func (c *creator) Counter() metrics.Counter {
	res := pro.NewCounter(pro.CounterOpts{
		Name: c.name,
		Help: c.helptext,
	})
	c.register(res)
	return res
}
func (c *creator) Gauge() metrics.Gauge {
	res := pro.NewGauge(pro.GaugeOpts{
		Name: c.name,
		Help: c.helptext,
	})
	c.register(res)
	return res
}
func (c *creator) Histogram(buckets []float64) metrics.Histogram {
	res := pro.NewHistogram(pro.HistogramOpts{
		Name:    c.name,
		Help:    c.helptext,
		Buckets: buckets,
	})
	c.register(res)
	return res
}

func (c *creator) Summary(opts metrics.SummaryOpts) metrics.Summary {
	res := pro.NewSummary(pro.SummaryOpts{
		Name: c.name,
		Help: c.helptext,

		Objectives: opts.Objectives,
		MaxAge:     opts.MaxAge,
		AgeBuckets: opts.AgeBuckets,
		BufCap:     opts.BufCap,
	})
	c.register(res)
	return res
}

func (c *creator) register(col pro.Collector) {
	err := pro.Register(col)
	if err != nil {
		log.Errorf("Registering prometheus collector, name: %s, error: %s\n", c.name, err.Error())
	}
}
