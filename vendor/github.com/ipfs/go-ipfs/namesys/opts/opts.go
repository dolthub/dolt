package nsopts

import (
	"time"
)

const (
	// DefaultDepthLimit is the default depth limit used by Resolve.
	DefaultDepthLimit = 32

	// UnlimitedDepth allows infinite recursion in Resolve.  You
	// probably don't want to use this, but it's here if you absolutely
	// trust resolution to eventually complete and can't put an upper
	// limit on how many steps it will take.
	UnlimitedDepth = 0
)

// ResolveOpts specifies options for resolving an IPNS path
type ResolveOpts struct {
	// Recursion depth limit
	Depth uint
	// The number of IPNS records to retrieve from the DHT
	// (the best record is selected from this set)
	DhtRecordCount uint
	// The amount of time to wait for DHT records to be fetched
	// and verified. A zero value indicates that there is no explicit
	// timeout (although there is an implicit timeout due to dial
	// timeouts within the DHT)
	DhtTimeout time.Duration
}

// DefaultResolveOpts returns the default options for resolving
// an IPNS path
func DefaultResolveOpts() ResolveOpts {
	return ResolveOpts{
		Depth:          DefaultDepthLimit,
		DhtRecordCount: 16,
		DhtTimeout:     time.Minute,
	}
}

// ResolveOpt is used to set an option
type ResolveOpt func(*ResolveOpts)

// Depth is the recursion depth limit
func Depth(depth uint) ResolveOpt {
	return func(o *ResolveOpts) {
		o.Depth = depth
	}
}

// DhtRecordCount is the number of IPNS records to retrieve from the DHT
func DhtRecordCount(count uint) ResolveOpt {
	return func(o *ResolveOpts) {
		o.DhtRecordCount = count
	}
}

// DhtTimeout is the amount of time to wait for DHT records to be fetched
// and verified. A zero value indicates that there is no explicit timeout
func DhtTimeout(timeout time.Duration) ResolveOpt {
	return func(o *ResolveOpts) {
		o.DhtTimeout = timeout
	}
}

// ProcessOpts converts an array of ResolveOpt into a ResolveOpts object
func ProcessOpts(opts []ResolveOpt) ResolveOpts {
	rsopts := DefaultResolveOpts()
	for _, option := range opts {
		option(&rsopts)
	}
	return rsopts
}
