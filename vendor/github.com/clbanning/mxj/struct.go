// Copyright 2012-2017 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package mxj

import (
	"encoding/json"
	"errors"
	"reflect"

	// "github.com/fatih/structs"
)

// Create a new Map value from a structure.  Error returned if argument is not a structure.
// Only public structure fields are decoded in the Map value. See github.com/fatih/structs#Map
// for handling of "structs" tags.

// DEPRECATED - import github.com/fatih/structs and cast result of structs.Map to mxj.Map.
//	import "github.com/fatih/structs"
//	...
//	   sm, err := structs.Map(<some struct>)
//	   if err != nil {
//	      // handle error
//	   }
//	   m := mxj.Map(sm)
// Alernatively uncomment the old source and import in struct.go.
func NewMapStruct(structVal interface{}) (Map, error) {
	return nil, errors.New("deprecated - see package documentation")
	/*
		if !structs.IsStruct(structVal) {
			return nil, errors.New("NewMapStruct() error: argument is not type Struct")
		}
		return structs.Map(structVal), nil
	*/
}

// Marshal a map[string]interface{} into a structure referenced by 'structPtr'. Error returned
// if argument is not a pointer or if json.Unmarshal returns an error.
//	json.Unmarshal structure encoding rules are followed to encode public structure fields.
func (mv Map) Struct(structPtr interface{}) error {
	// should check that we're getting a pointer.
	if reflect.ValueOf(structPtr).Kind() != reflect.Ptr {
		return errors.New("mv.Struct() error: argument is not type Ptr")
	}

	m := map[string]interface{}(mv)
	j, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return json.Unmarshal(j, structPtr)
}
