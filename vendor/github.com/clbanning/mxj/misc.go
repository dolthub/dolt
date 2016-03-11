// Copyright 2016 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// misc.go - mimic functions (+others) called out in:
//          https://groups.google.com/forum/#!topic/golang-nuts/jm_aGsJNbdQ
// Primarily these methods let you retrive XML structure information.

package mxj

import (
	"fmt"
	"sort"
)

// Return the root element of the Map. If there is not a single key in Map,
// then an error is returned.
func (m Map) Root() (string, error) {
	mm := map[string]interface{}(m)
	if len(mm) != 1 {
		return "", fmt.Errorf("Map does not have singleton root. Len: %d.", len(mm))
	}
	for k, _ := range mm {
		return k, nil
	}
	return "", nil
}

// If the path is an element with sub-elements, return a list of the sub-element
// keys.  (The list is alphabeticly sorted.)  NOTE: Map keys that are prefixed with
// '-', a hyphen, are considered attributes; see m.Attributes(path).
func (m Map) Elements(path string) ([]string, error) {
	e, err := m.ValueForPath(path)
	if err != nil {
		return nil, err
	}
	switch e.(type) {
	case map[string]interface{}:
		ee := e.(map[string]interface{})
		elems := make([]string, len(ee))
		var i int
		for k, _ := range ee {
			if k[:1] == "-" {
				continue // skip attributes
			}
			elems[i] = k
			i++
		}
		elems = elems[:i]
		// alphabetic sort keeps things tidy
		sort.Strings(elems)
		return elems, nil
	}
	return nil, fmt.Errorf("no elements for path: %s", path)
}

// If the path is an element with attributes, return a list of the attribute
// keys.  (The list is alphabeticly sorted.)  NOTE: Map keys that are not prefixed with
// '-', a hyphen, are not treated as attributes; see m.Elements(path).
func (m Map) Attributes(path string) ([]string, error) {
	a, err := m.ValueForPath(path)
	if err != nil {
		return nil, err
	}
	switch a.(type) {
	case map[string]interface{}:
		aa := a.(map[string]interface{})
		attrs := make([]string, len(aa))
		var i int
		for k, _ := range aa {
			if k[:1] != "-" {
				continue // skip non-attributes
			}
			attrs[i] = k[1:]
			i++
		}
		attrs = attrs[:i]
		// alphabetic sort keeps things tidy
		sort.Strings(attrs)
		return attrs, nil
	}
	return nil, fmt.Errorf("no attributes for path: %s", path)
}
