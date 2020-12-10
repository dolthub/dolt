// Copyright 2019 Dolthub, Inc.
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

package pipeline

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
)

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

// RowWithProps is a struct that couples a row being processed by a pipeline with properties.  These properties work as
// a means of passing data about a row between stages in a pipeline.
type RowWithProps struct {
	Row   row.Row
	Props ImmutableProperties
}

// NewRowWithProps creates a RowWith props from a row and a map of properties
func NewRowWithProps(r row.Row, props map[string]interface{}) RowWithProps {
	return RowWithProps{r, ImmutableProperties{props}}
}

// GetRowConvTranformFunc can be used to wrap a RowConverter and use that RowConverter in a pipeline.
func GetRowConvTransformFunc(rc *rowconv.RowConverter) func(row.Row, ReadableMap) ([]*TransformedRowResult, string) {
	if rc.IdentityConverter {
		return func(inRow row.Row, props ReadableMap) (outRows []*TransformedRowResult, badRowDetails string) {
			return []*TransformedRowResult{{RowData: inRow, PropertyUpdates: nil}}, ""
		}
	} else {
		return func(inRow row.Row, props ReadableMap) (outRows []*TransformedRowResult, badRowDetails string) {
			outRow, err := rc.Convert(inRow)

			if err != nil {
				return nil, err.Error()
			}

			if isv, err := row.IsValid(outRow, rc.DestSch); err != nil {
				return nil, err.Error()
			} else if !isv {
				col, err := row.GetInvalidCol(outRow, rc.DestSch)

				if err != nil {
					return nil, "invalid column"
				} else {
					return nil, "invalid column: " + col.Name
				}
			}

			return []*TransformedRowResult{{RowData: outRow, PropertyUpdates: nil}}, ""
		}
	}
}
