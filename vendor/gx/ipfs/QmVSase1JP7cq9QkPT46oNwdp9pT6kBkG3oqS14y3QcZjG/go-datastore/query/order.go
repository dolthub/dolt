package query

import (
	"sort"
)

// Order is an object used to order objects
type Order interface {

	// Sort sorts the Entry slice according to
	// the Order criteria.
	Sort([]Entry)
}

// OrderByValue is used to signal to datastores they
// should apply internal orderings. unfortunately, there
// is no way to apply order comparisons to interface{} types
// in Go, so if the datastore doesnt have a special way to
// handle these comparisons, you must provide an Order
// implementation that casts to the correct type.
type OrderByValue struct {
	TypedOrder Order
}

func (o OrderByValue) Sort(res []Entry) {
	if o.TypedOrder == nil {
		panic("cannot order interface{} by value. see query docs.")
	}
	o.TypedOrder.Sort(res)
}

// OrderByValueDescending is used to signal to datastores they
// should apply internal orderings. unfortunately, there
// is no way to apply order comparisons to interface{} types
// in Go, so if the datastore doesnt have a special way to
// handle these comparisons, you are SOL.
type OrderByValueDescending struct {
	TypedOrder Order
}

func (o OrderByValueDescending) Sort(res []Entry) {
	if o.TypedOrder == nil {
		panic("cannot order interface{} by value. see query docs.")
	}
	o.TypedOrder.Sort(res)
}

// OrderByKey
type OrderByKey struct{}

func (o OrderByKey) Sort(res []Entry) {
	sort.Stable(reByKey(res))
}

// OrderByKeyDescending
type OrderByKeyDescending struct{}

func (o OrderByKeyDescending) Sort(res []Entry) {
	sort.Stable(sort.Reverse(reByKey(res)))
}

type reByKey []Entry

func (s reByKey) Len() int           { return len(s) }
func (s reByKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s reByKey) Less(i, j int) bool { return s[i].Key < s[j].Key }
