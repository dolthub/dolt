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

package config

import (
	"errors"
	"strconv"
)

// ErrConfigParamNotFound - Error returned when the config does not contain the parameter requested
var ErrConfigParamNotFound = errors.New("Param not found")

// ReadableConfig interface provides a mechanisms for getting key value pairs from a config
type ReadableConfig interface {

	// GetString retrieves a string given a key.  If there is no config property with the given key then
	// config.ErrConfigParamNotFound will be returned. Other errors may be returned depending on the
	// ReadableConfig implementation.
	GetString(key string) (value string, err error)

	// Iter will perform a callback for each value in a config until all values have been exhausted or until the
	// callback returns true indicating that it should stop.
	Iter(func(string, string) (stop bool))

	// Size returns the number of properties contained within the config
	Size() int
}

// WritableConfig interface provides a mechanism for setting key value pairs in a config
type WritableConfig interface {

	// SetStrings uses the updates map to set configuration parameter values.
	SetStrings(updates map[string]string) error

	// Unset removes a configuration parameter from the config
	Unset(params []string) error
}

// ReadWriteConfig interface provides a mechanism for both getting and setting key value pairs in a config
type ReadWriteConfig interface {
	ReadableConfig
	WritableConfig
}

// GetString retrieves a string value from a ReadableConfig
func GetString(cs ReadableConfig, k string) (string, error) {
	return cs.GetString(k)
}

// GetInt retrieves a string value from a ReadableConfig and converts it to an integer.
func GetInt(cs ReadableConfig, k string) (int64, error) {
	if s, err := cs.GetString(k); err == nil {
		if val, err := strconv.ParseInt(s, 10, 64); err != nil {
			return 0, err
		} else {
			return val, nil
		}
	} else {
		return 0, err
	}
}

// GetUint retrieves a string value from a ReadableConfig and converts it to an unsigned integer.
func GetUint(cs ReadableConfig, k string) (uint64, error) {
	if s, err := cs.GetString(k); err == nil {
		if val, err := strconv.ParseUint(s, 10, 64); err != nil {
			return 0, err
		} else {
			return val, nil
		}
	} else {
		return 0, err
	}
}

// GetFloat retrieves a string value from a ReadableConfig and converts it to a float.
func GetFloat(cs ReadableConfig, k string) (float64, error) {
	if s, err := cs.GetString(k); err == nil {
		if val, err := strconv.ParseFloat(s, 64); err != nil {
			return 0, err
		} else {
			return val, nil
		}
	} else {
		return 0, err
	}
}

// SetStrings sets configuration values from the values in the updates map
func SetStrings(c WritableConfig, updates map[string]string) error {
	return c.SetStrings(updates)
}

// SetInt sets a value in the WritableConfig for a given key to the string converted value of an integer
func SetInt(c WritableConfig, key string, val int64) error {
	s := strconv.FormatInt(val, 10)
	return c.SetStrings(map[string]string{key: s})
}

// SetUint sets a value in the Writable for a given key to the string converted value of an unsigned int
func SetUint(c WritableConfig, key string, val uint64) error {
	s := strconv.FormatUint(val, 10)
	return c.SetStrings(map[string]string{key: s})
}

// SetFloat sets a value in the WritableConfig for a given key to the string converted value of a float
func SetFloat(c WritableConfig, key string, val float64) error {
	s := strconv.FormatFloat(val, byte('f'), 8, 64)
	return c.SetStrings(map[string]string{key: s})
}

// Equals compares a config against a map and returns whether the map contains all the values of the
// config with the same values.
func Equals(cfg ReadableConfig, compareProps map[string]string) bool {
	if cfg.Size() != len(compareProps) {
		return false
	}

	isEqual := true
	cfg.Iter(func(name, value string) (stop bool) {
		if compareVal, ok := compareProps[name]; ok {
			isEqual = (compareVal == value)
		} else {
			isEqual = false
		}

		return !isEqual
	})

	return isEqual
}
