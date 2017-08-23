package query

import (
	"fmt"
	"reflect"
	"strings"
)

// Filter is an object that tests ResultEntries
type Filter interface {
	// Filter returns whether an entry passes the filter
	Filter(e Entry) bool
}

// Op is a comparison operator
type Op string

var (
	Equal              = Op("==")
	NotEqual           = Op("!=")
	GreaterThan        = Op(">")
	GreaterThanOrEqual = Op(">=")
	LessThan           = Op("<")
	LessThanOrEqual    = Op("<=")
)

// FilterValueCompare is used to signal to datastores they
// should apply internal comparisons. unfortunately, there
// is no way to apply comparisons* to interface{} types in
// Go, so if the datastore doesnt have a special way to
// handle these comparisons, you must provided the
// TypedFilter to actually do filtering.
//
// [*] other than == and !=, which use reflect.DeepEqual.
type FilterValueCompare struct {
	Op          Op
	Value       interface{}
	TypedFilter Filter
}

func (f FilterValueCompare) Filter(e Entry) bool {
	if f.TypedFilter != nil {
		return f.TypedFilter.Filter(e)
	}

	switch f.Op {
	case Equal:
		return reflect.DeepEqual(f.Value, e.Value)
	case NotEqual:
		return !reflect.DeepEqual(f.Value, e.Value)
	default:
		panic(fmt.Errorf("cannot apply op '%s' to interface{}.", f.Op))
	}
}

type FilterKeyCompare struct {
	Op  Op
	Key string
}

func (f FilterKeyCompare) Filter(e Entry) bool {
	switch f.Op {
	case Equal:
		return e.Key == f.Key
	case NotEqual:
		return e.Key != f.Key
	case GreaterThan:
		return e.Key > f.Key
	case GreaterThanOrEqual:
		return e.Key >= f.Key
	case LessThan:
		return e.Key < f.Key
	case LessThanOrEqual:
		return e.Key <= f.Key
	default:
		panic(fmt.Errorf("unknown op '%s'", f.Op))
	}
}

type FilterKeyPrefix struct {
	Prefix string
}

func (f FilterKeyPrefix) Filter(e Entry) bool {
	return strings.HasPrefix(e.Key, f.Prefix)
}
