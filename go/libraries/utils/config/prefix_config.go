// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"strings"
)

// PrefixConfig decorates read and write access to the underlying config by appending a prefix to the accessed keys
// on reads and writes
// TODO: this is temporary to namespace server configs in .dolt/config.json
// TODO: separate CLI and SQL configs and delete this class
type PrefixConfig struct {
	c      ReadWriteConfig
	prefix string
}

func NewPrefixConfig(cfg ReadWriteConfig, prefix string) PrefixConfig {
	return PrefixConfig{c: cfg, prefix: prefix}
}

func (nsc PrefixConfig) path(key string) string {
	return fmt.Sprintf("%s.%s", nsc.prefix, key)
}

func (nsc PrefixConfig) GetString(key string) (value string, err error) {
	return nsc.c.GetString(nsc.path(key))
}

func (nsc PrefixConfig) GetStringOrDefault(key, defStr string) string {
	return nsc.c.GetStringOrDefault(nsc.path(key), defStr)
}

func (nsc PrefixConfig) SetStrings(updates map[string]string) error {
	for k, v := range updates {
		delete(updates, k)
		updates[nsc.path(k)] = v
	}
	return nsc.c.SetStrings(updates)
}

func (nsc PrefixConfig) Iter(cb func(string, string) (stop bool)) {
	nsc.c.Iter(func(k, v string) (stop bool) {
		if strings.HasPrefix(k, nsc.prefix+".") {
			return cb(strings.TrimPrefix(k, nsc.prefix+"."), v)
		}
		return false
	})
	return
}

func (nsc PrefixConfig) Size() int {
	count := 0
	nsc.Iter(func(k, v string) (stop bool) {
		count += 1
		return false
	})
	return count
}

func (nsc PrefixConfig) Unset(params []string) error {
	for i, k := range params {
		params[i] = nsc.path(k)
	}
	return nsc.c.Unset(params)
}
