package metrics

import (
	"sync"

	peer "gx/ipfs/QmXYjuNuxVzXKJCfWasQk1RqkhVLDM9jtUKhqc2WPQmFSB/go-libp2p-peer"
	protocol "gx/ipfs/QmZNkThpqfVXs9GNbexPrfBbXSLNYeKrE7jwFM2oqHbyqN/go-libp2p-protocol"
	gm "gx/ipfs/QmeYJHEk8UjVVZ4XCRTZe6dFQrb8pGWD81LYCgeLp8CvMB/go-metrics"
)

type Stats struct {
	TotalIn  int64
	TotalOut int64
	RateIn   float64
	RateOut  float64
}

type BandwidthCounter struct {
	lock     sync.Mutex
	totalIn  gm.Meter
	totalOut gm.Meter
	reg      gm.Registry
}

func NewBandwidthCounter() *BandwidthCounter {
	reg := gm.NewRegistry()
	return &BandwidthCounter{
		totalIn:  gm.GetOrRegisterMeter("totalIn", reg),
		totalOut: gm.GetOrRegisterMeter("totalOut", reg),
		reg:      reg,
	}
}

func (bwc *BandwidthCounter) LogSentMessage(size int64) {
	bwc.totalOut.Mark(size)
}

func (bwc *BandwidthCounter) LogRecvMessage(size int64) {
	bwc.totalIn.Mark(size)
}

func (bwc *BandwidthCounter) LogSentMessageStream(size int64, proto protocol.ID, p peer.ID) {
	meter := gm.GetOrRegisterMeter("/peer/out/"+string(p), bwc.reg)
	meter.Mark(size)

	pmeter := gm.GetOrRegisterMeter("/proto/out/"+string(proto), bwc.reg)
	pmeter.Mark(size)
}

func (bwc *BandwidthCounter) LogRecvMessageStream(size int64, proto protocol.ID, p peer.ID) {
	meter := gm.GetOrRegisterMeter("/peer/in/"+string(p), bwc.reg)
	meter.Mark(size)

	pmeter := gm.GetOrRegisterMeter("/proto/in/"+string(proto), bwc.reg)
	pmeter.Mark(size)
}

func (bwc *BandwidthCounter) GetBandwidthForPeer(p peer.ID) (out Stats) {
	inMeter := gm.GetOrRegisterMeter("/peer/in/"+string(p), bwc.reg).Snapshot()
	outMeter := gm.GetOrRegisterMeter("/peer/out/"+string(p), bwc.reg).Snapshot()

	return Stats{
		TotalIn:  inMeter.Count(),
		TotalOut: outMeter.Count(),
		RateIn:   inMeter.RateFine(),
		RateOut:  outMeter.RateFine(),
	}
}

func (bwc *BandwidthCounter) GetBandwidthForProtocol(proto protocol.ID) (out Stats) {
	inMeter := gm.GetOrRegisterMeter(string("/proto/in/"+proto), bwc.reg).Snapshot()
	outMeter := gm.GetOrRegisterMeter(string("/proto/out/"+proto), bwc.reg).Snapshot()

	return Stats{
		TotalIn:  inMeter.Count(),
		TotalOut: outMeter.Count(),
		RateIn:   inMeter.RateFine(),
		RateOut:  outMeter.RateFine(),
	}
}

func (bwc *BandwidthCounter) GetBandwidthTotals() (out Stats) {
	return Stats{
		TotalIn:  bwc.totalIn.Count(),
		TotalOut: bwc.totalOut.Count(),
		RateIn:   bwc.totalIn.RateFine(),
		RateOut:  bwc.totalOut.RateFine(),
	}
}
