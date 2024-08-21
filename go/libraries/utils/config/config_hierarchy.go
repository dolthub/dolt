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

package config

import (
	"errors"
	"strings"
)

const (
	namespaceSep = "::"
)

var ErrUnknownConfig = errors.New("config not found")

// ConfigHierarchy is a hierarchical read-only configuration store.  When a key is looked up in the ConfigHierarchy it
// will go through its configs in order and will return the first value for a given key that is found.  Configs are
// iterated in order, so the configurations added first have the highest priority.
type ConfigHierarchy struct {
	configs      []ReadWriteConfig
	nameToConfig map[string]ReadWriteConfig
}

var _ ReadableConfig = &ConfigHierarchy{}
var _ WritableConfig = &ConfigHierarchy{}
var _ ReadWriteConfig = &ConfigHierarchy{}

// NewConfigHierarchy creates an empty ConfigurationHierarchy
func NewConfigHierarchy() *ConfigHierarchy {
	return &ConfigHierarchy{[]ReadWriteConfig{}, map[string]ReadWriteConfig{}}
}

// AddConfig adds a ReadWriteConfig to the hierarchy.  Newly added configs are at a lower priority than the configs that
// were added previously.  Though the ConfigHierarchy does not support modification of stored values in the configs
// directly, the configs it manages must implement the WritableConfig interface.
func (ch *ConfigHierarchy) AddConfig(name string, cs ReadWriteConfig) {
	name = strings.TrimSpace(strings.ToLower(name))

	if _, ok := ch.nameToConfig[name]; ok {
		panic("Adding 2 configs with the same name is not a valid operation.")
	}

	ch.configs = append(ch.configs, cs)
	ch.nameToConfig[name] = cs
}

// GetConfig retrieves a config by name. ReadWriteConfig instances can be modified
func (ch *ConfigHierarchy) GetConfig(name string) (ReadWriteConfig, bool) {
	name = strings.TrimSpace(strings.ToLower(name))

	cs, ok := ch.nameToConfig[name]
	return cs, ok
}

// GetString iterates through all configs in the order they were added looking for a given key.  The first valid value
// found will be returned.
func (ch *ConfigHierarchy) GetString(k string) (string, error) {
	ns, paramName := splitParamName(k)

	if ns != "" {
		if cfg, ok := ch.nameToConfig[ns]; ok {
			return cfg.GetString(paramName)
		} else {
			return "", errors.New("Hierarchy does not have a config named " + ns)
		}
	}

	for _, cs := range ch.configs {
		val, err := cs.GetString(k)

		if err != nil && err != ErrConfigParamNotFound {
			return "", err
		} else if err == nil {
			return val, nil
		}
	}

	return "", ErrConfigParamNotFound
}

func (ch *ConfigHierarchy) GetStringOrDefault(key, defStr string) string {
	if val, err := ch.GetString(key); err == nil {
		return val
	}
	return defStr
}

// SetStrings will set the value of configuration parameters in memory, and persist any changes to the backing file.
// For ConfigHierarchies update parameter names must be of the format config_name::param_name
func (ch *ConfigHierarchy) SetStrings(updates map[string]string) error {
	namespacedUpdates := make(map[string]map[string]string)
	for cfgName := range ch.nameToConfig {
		namespacedUpdates[cfgName] = make(map[string]string)
	}

	for k, v := range updates {
		ns, paramName := splitParamName(k)

		if ns == "" {
			// panicking in cases where developers have used this function incorrectly
			panic("Calls to SetStrings for a ConfigHierarchy must include the config name. " + k + " is not in the format config_name::param_name")
		}

		if _, ok := namespacedUpdates[ns]; !ok {
			return errors.New(ns + " is not a known config in this hierarchy.")
		} else {
			namespacedUpdates[ns][paramName] = v
		}
	}

	for ns, updates := range namespacedUpdates {
		err := ch.nameToConfig[ns].SetStrings(updates)

		if err != nil {
			return err
		}
	}

	return nil
}

func splitParamName(paramName string) (string, string) {
	tokens := strings.Split(paramName, namespaceSep)

	nonEmpty := make([]string, 0, len(tokens))
	for _, token := range tokens {
		trimmed := strings.TrimSpace(strings.ToLower(token))

		if len(trimmed) > 0 {
			nonEmpty = append(nonEmpty, trimmed)
		}
	}

	switch len(nonEmpty) {
	case 0:
		return "", ""
	case 1:
		return "", nonEmpty[0]
	default:
		return nonEmpty[0], strings.Join(nonEmpty[1:], "::")
	}
}

// Iter will perform a callback for each value in a config until all values have been exhausted or until the
// callback returns true indicating that it should stop. For ConfigHierchies, parameter names which are provided
// to the callbacks are of the format config_name::param_name
func (ch *ConfigHierarchy) Iter(cb func(string, string) (stop bool)) {
	stop := false
	for cfgName, cfg := range ch.nameToConfig {
		cfg.Iter(func(cfgParamName string, cfgParamVal string) bool {
			stop = cb(cfgName+namespaceSep+cfgParamName, cfgParamVal)
			return stop
		})

		if stop {
			break
		}
	}
}

// Unset removes a configuration parameter from the config
func (ch *ConfigHierarchy) Unset(params []string) error {
	namespacedDeletes := make(map[string][]string)

	for cfgName := range ch.nameToConfig {
		namespacedDeletes[cfgName] = []string{}
	}

	for _, param := range params {
		ns, paramName := splitParamName(param)

		if ns == "" {
			// panicking in cases where developers have used this function incorrectly
			panic("Calls to Unset for a ConfigHierarchy must include the config name. " + param + " is not in the format config_name::param_name")
		}

		if _, ok := namespacedDeletes[ns]; !ok {
			return errors.New(ns + " is not a known config in this hierarchy.")
		} else {
			namespacedDeletes[ns] = append(namespacedDeletes[ns], paramName)
		}
	}

	for ns, deletes := range namespacedDeletes {
		err := ch.nameToConfig[ns].Unset(deletes)

		if err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of properties contained within the config. For config hierarchy it returns the sum of the
// sizes of all elements in the hierarchy. This is the number of elements that would be seen when calling Iter on
// the hierarchy.
func (ch *ConfigHierarchy) Size() int {
	size := 0
	for _, cfg := range ch.configs {
		size += cfg.Size()
	}

	return size
}
