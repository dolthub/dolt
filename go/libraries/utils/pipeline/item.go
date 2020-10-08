package pipeline

import "sync/atomic"

// Copyright 2019 Liquidata, Inc.
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

// ReadableMap is an interface that provides read only access to map properties
type ReadableMap interface {
	// Get retrieves an element from the map, and a bool which says if there was a property that exists with that
	// name at all
	Get(propName string) (interface{}, bool)
}

// NoProps is an empty ImmutableProperties struct
var NoProps = ImmutableProperties{}

// ImmutableProperties is a map of properties which can't be edited after creation
type ImmutableProperties struct {
	props map[string]interface{}
}

// Get retrieves an element from the map, and a bool which says if there was a property that exists with that name at all
func (ip ImmutableProperties) Get(propName string) (interface{}, bool) {
	if ip.props == nil {
		return nil, false
	}

	val, ok := ip.props[propName]
	return val, ok
}

// Set will create a new ImmutableProperties struct whose values are the original properties combined with the provided
// updates
func (ip ImmutableProperties) Set(updates map[string]interface{}) ImmutableProperties {
	numProps := len(updates) + len(ip.props)
	allProps := make(map[string]interface{}, numProps)

	for k, v := range ip.props {
		allProps[k] = v
	}

	for k, v := range updates {
		allProps[k] = v
	}

	return ImmutableProperties{allProps}
}

type ItemWithProps interface {
	GetItem() interface{}
	GetProperties() ReadableMap
}

type itemWithProps struct {
	item  interface{}
	props ImmutableProperties
}

func (iwp itemWithProps) GetItem() interface{} {
	return iwp.item
}

func (iwp itemWithProps) GetProperties() ReadableMap {
	return iwp.props
}

func NewItemWithNoProps(item interface{}) ItemWithProps {
	return itemWithProps{item, NoProps}
}

// NewItemWithProps creates an item with props from an item and a map of properties
func NewItemWithProps(item interface{}, props map[string]interface{}) ItemWithProps {
	return itemWithProps{item, ImmutableProperties{props}}
}

type ErrorItem struct {
	Err       error
	ErrNumber uint32
}

func NewErrorItem(err error, errNum *uint32) ErrorItem {
	return ErrorItem{
		Err:       err,
		ErrNumber: atomic.AddUint32(errNum, 1),
	}
}

func (ei ErrorItem) GetItem() interface{} {
	return nil
}

func (ei ErrorItem) GetProperties() ReadableMap {
	return NoProps
}

func (ei ErrorItem) Error() string {
	return ei.Err.Error()
}
