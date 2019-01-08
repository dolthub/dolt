// mxj - A collection of map[string]interface{} and associated XML and JSON utilities.
// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package mxj

import (
	"fmt"
	"sort"
)

const (
	Cast         = true // for clarity - e.g., mxj.NewMapXml(doc, mxj.Cast)
	SafeEncoding = true // ditto - e.g., mv.Json(mxj.SafeEncoding)
)

type Map map[string]interface{}

// Allocate a Map.
func New() Map {
	m := make(map[string]interface{}, 0)
	return m
}

// Cast a Map to map[string]interface{}
func (mv Map) Old() map[string]interface{} {
	return mv
}

// Return a copy of mv as a newly allocated Map.  If the Map only contains string,
// numeric, map[string]interface{}, and []interface{} values, then it can be thought
// of as a "deep copy."  Copying a structure (or structure reference) value is subject
// to the noted restrictions.
//	NOTE: If 'mv' includes structure values with, possibly, JSON encoding tags
//	      then only public fields of the structure are in the new Map - and with
//	      keys that conform to any encoding tag instructions. The structure itself will
//	      be represented as a map[string]interface{} value.
func (mv Map) Copy() (Map, error) {
	// this is the poor-man's deep copy
	// not efficient, but it works
	j, jerr := mv.Json()
	// must handle, we don't know how mv got built
	if jerr != nil {
		return nil, jerr
	}
	return NewMapJson(j)
}

// --------------- StringIndent ... from x2j.WriteMap -------------

// Pretty print a Map.
func (mv Map) StringIndent(offset ...int) string {
	return writeMap(map[string]interface{}(mv), true, true, offset...)
}

// Pretty print a Map without the value type information - just key:value entries.
func (mv Map) StringIndentNoTypeInfo(offset ...int) string {
	return writeMap(map[string]interface{}(mv), false, true, offset...)
}

// writeMap - dumps the map[string]interface{} for examination.
// 'typeInfo' causes value type to be printed.
//	'offset' is initial indentation count; typically: Write(m).
func writeMap(m interface{}, typeInfo, root bool, offset ...int) string {
	var indent int
	if len(offset) == 1 {
		indent = offset[0]
	}

	var s string
	switch m.(type) {
	case []interface{}:
		if typeInfo {
			s += "[[]interface{}]"
		}
		for _, v := range m.([]interface{}) {
			s += "\n"
			for i := 0; i < indent; i++ {
				s += "  "
			}
			s += writeMap(v, typeInfo, false, indent+1)
		}
	case map[string]interface{}:
		list := make([][2]string, len(m.(map[string]interface{})))
		var n int
		for k, v := range m.(map[string]interface{}) {
			list[n][0] = k
			list[n][1] = writeMap(v, typeInfo, false, indent+1)
			n++
		}
		sort.Sort(mapList(list))
		for _, v := range list {
			if root {
				root = false
			} else {
				s += "\n"
			}
			for i := 0; i < indent; i++ {
				s += "  "
			}
			s += v[0] + " : " + v[1]
		}
	default:
		if typeInfo {
			s += fmt.Sprintf("[%T] %+v", m, m)
		} else {
			s += fmt.Sprintf("%+v", m)
		}
	}
	return s
}

// ======================== utility ===============

type mapList [][2]string

func (ml mapList) Len() int {
	return len(ml)
}

func (ml mapList) Swap(i, j int) {
	ml[i], ml[j] = ml[j], ml[i]
}

func (ml mapList) Less(i, j int) bool {
	return ml[i][0] <= ml[j][0]
}
