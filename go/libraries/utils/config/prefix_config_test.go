// Copyright 2020 Dolthub, Inc.
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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

var ConfigVals = map[string]string{
	"scopeA.k1": "v1",
	"scopeA.k2": "v2",
	"scopeB.k3": "v3",
	"k1":        "v1",
}

func newConfigVals() map[string]string {
	newConfig := make(map[string]string)
	for k, v := range ConfigVals {
		newConfig[k] = v
	}
	return newConfig
}

func newPrefixConfig(prefix string) PrefixConfig {

	mc := NewMapConfig(newConfigVals())
	return NewPrefixConfig(mc, prefix)

}

func TestPrefixConfigSet(t *testing.T) {
	conf := newPrefixConfig("test")
	conf.SetStrings(newConfigVals())
	v1, _ := conf.c.GetString("test.k1")
	assert.Equal(t, v1, "v1")
}

func TestPrefixConfigGet(t *testing.T) {
	t.Run("test GetString", func(t *testing.T) {
		conf := newPrefixConfig("scopeA")
		v1, _ := conf.GetString("k1")
		assert.Equal(t, "v1", v1)
	})

	t.Run("test GetString fails out of scope", func(t *testing.T) {
		conf := newPrefixConfig("scopeA")
		_, err := conf.GetString("k3")
		assert.Equal(t, err, ErrConfigParamNotFound)
	})

	t.Run("test GetStringofDefault", func(t *testing.T) {
		conf := newPrefixConfig("scopeA")
		v1, _ := conf.GetString("k1")
		assert.Equal(t, "v1", v1)
	})

	t.Run("test GetStringOrDefault fails out of scope", func(t *testing.T) {
		conf := newPrefixConfig("scopeA")
		res := conf.GetStringOrDefault("k3", "default")
		assert.Equal(t, "default", res)
	})
}

func TestPrefixConfigUnset(t *testing.T) {
	t.Run("test Unset", func(t *testing.T) {
		conf := newPrefixConfig("scopeA")
		err := conf.Unset([]string{"k1"})
		assert.NoError(t, err)
		res := conf.GetStringOrDefault("k3", "default")
		assert.Equal(t, "default", res)
	})

	t.Run("test Unset doesn't affect other scope", func(t *testing.T) {
		conf := newPrefixConfig("scopeA")
		err := conf.Unset([]string{"k1"})
		assert.NoError(t, err)
		res := conf.c.GetStringOrDefault("k1", "")
		assert.Equal(t, "v1", res)
	})
}

func TestPrefixConfigSize(t *testing.T) {
	conf := newPrefixConfig("scopeA")
	size := conf.Size()
	assert.Equal(t, size, 2)
}

func TestPrefixConfigIter(t *testing.T) {
	conf := newPrefixConfig("scopeA")
	keys := make([]string, 0, 6)
	conf.Iter(func(k, v string) bool {
		keys = append(keys, k)
		return false
	})
	sort.Strings(keys)
	assert.Equal(t, []string{"k1", "k2"}, keys)
}
