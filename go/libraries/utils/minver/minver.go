// Copyright 2024 Dolthub, Inc.
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

package minver

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/dolthub/dolt/go/libraries/utils/version"
)

func YamlForVersion(st any, versionNum uint32) ([]byte, error) {
	err := NullUnsupported(versionNum, st)
	if err != nil {
		return nil, fmt.Errorf("error nulling unsupported fields for version %d: %w", versionNum, err)
	}

	return yaml.Marshal(st)
}

func NullUnsupported(verNum uint32, st any) error {
	const tagName = "minver"

	// use reflection to loop over all fields in the struct st
	// for each field check the tag "minver" and if the current version is less than that, set the field to nil
	t := reflect.TypeOf(st)

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("expected a pointer to a struct, got %T", st)
	} else if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Iterate over all available fields and read the tag value
	for i := 0; i < t.NumField(); i++ {
		// Get the field, returns https://golang.org/pkg/reflect/#StructField
		field := t.Field(i)

		// Get the field tag value
		tag := field.Tag.Get(tagName)

		if tag != "" {
			// if it's nullable check to see if it should be set to nil
			if field.Type.Kind() == reflect.Ptr || field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Map {
				var setToNull bool

				if tag == "TBD" {
					setToNull = true
				} else {
					minver, err := version.Encode(tag)
					if err != nil {
						return fmt.Errorf("invalid version tag '%s' on field '%s': %w", tag, field.Name, err)
					}

					setToNull = verNum < minver
				}

				if setToNull {
					// Get the field value
					v := reflect.ValueOf(st).Elem().Field(i)
					v.Set(reflect.Zero(v.Type()))
				}
			} else {
				return fmt.Errorf("non-nullable field '%s' has a version tag '%s'", field.Name, tag)
			}

			var hasOmitEmpty bool
			yamlTag := field.Tag.Get("yaml")
			if yamlTag != "" {
				vals := strings.Split(yamlTag, ",")
				for _, val := range vals {
					if val == "omitempty" {
						hasOmitEmpty = true
						break
					}
				}
			}

			if !hasOmitEmpty {
				return fmt.Errorf("field '%s' has a version tag '%s' but no yaml tag with omitempty", field.Name, tag)
			}
		}

		v := reflect.ValueOf(st).Elem().Field(i)

		vIsNullable := v.Type().Kind() == reflect.Ptr || v.Type().Kind() == reflect.Slice || v.Type().Kind() == reflect.Map

		if !vIsNullable || !v.IsNil() {
			// if the field is a pointer to a struct, or a struct, or a slice recurse
			if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
				err := NullUnsupported(verNum, v.Interface())
				if err != nil {
					return err
				}
			} else if field.Type.Kind() == reflect.Struct {
				err := NullUnsupported(verNum, v.Addr().Interface())
				if err != nil {
					return err
				}
			} else if field.Type.Kind() == reflect.Slice {
				if field.Type.Elem().Kind() == reflect.Ptr && field.Type.Elem().Elem().Kind() == reflect.Struct {
					for i := 0; i < v.Len(); i++ {
						err := NullUnsupported(verNum, v.Index(i).Interface())
						if err != nil {
							return err
						}
					}
				} else if field.Type.Elem().Kind() == reflect.Struct {
					for i := 0; i < v.Len(); i++ {
						err := NullUnsupported(verNum, v.Index(i).Addr().Interface())
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}
