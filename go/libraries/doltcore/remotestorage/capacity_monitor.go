// Copyright 2019-2020 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
