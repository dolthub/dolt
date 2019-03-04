package pipeline

import "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"

type ReadableMap interface {
	Get(propName string) (interface{}, bool)
}

var NoProps = ImmutableProperties{}

type ImmutableProperties struct {
	props map[string]interface{}
}

func (ip ImmutableProperties) Get(propName string) (interface{}, bool) {
	if ip.props == nil {
		return nil, false
	}

	val, ok := ip.props[propName]
	return val, ok
}

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

type RowWithProps struct {
	Row   row.Row
	Props ImmutableProperties
}

func NewRowWithProps(r row.Row, props map[string]interface{}) RowWithProps {
	return RowWithProps{r, ImmutableProperties{props}}
}
