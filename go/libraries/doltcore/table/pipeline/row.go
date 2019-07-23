package pipeline

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"

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
