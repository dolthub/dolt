package remotestorage

// CapacityMonitor returns true if a capacity is exceeded
type CapacityMonitor interface {
	CapacityExceeded(size int) bool
}

type uncapped struct{}

var _ CapacityMonitor = &uncapped{}

func (cap *uncapped) CapacityExceeded(size int) bool {
	return false
}

func NewUncappedCapacityMonitor() *uncapped {
	return &uncapped{}
}

type fixedCapacity struct {
	capacity int64
	currSize int64
}

var _ CapacityMonitor = &fixedCapacity{}

func (cap *fixedCapacity) CapacityExceeded(size int) bool {
	cap.currSize += int64(size)
	return cap.currSize > cap.capacity
}

func NewFixedCapacityMonitor(maxCapacity int64) *fixedCapacity {
	return &fixedCapacity{capacity: maxCapacity}
}
