// Copyright 2026 Dolthub, Inc.
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

package dsess

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestCachedViewDefinitions(t *testing.T) {
	c := newSessionCache()
	key := doltdb.DataCacheKey{Hash: hash.Of([]byte("dolt_schemas"))}
	otherKey := doltdb.DataCacheKey{Hash: hash.Of([]byte("other"))}

	_, found, cached := c.GetCachedViewDefinition(key, "s1", "v")
	assert.False(t, found)
	assert.False(t, cached)

	v := sql.ViewDefinition{Name: "V", CreateViewStatement: "create view V as select 1"}
	c.CacheViews(key, []sql.ViewDefinition{v}, "S1")

	got, found, cached := c.GetCachedViewDefinition(key, "s1", "v")
	assert.True(t, found)
	assert.True(t, cached)
	assert.Equal(t, v, got)

	// A name that isn't a view is answered from the cache.
	_, found, cached = c.GetCachedViewDefinition(key, "s1", "t")
	assert.False(t, found)
	assert.True(t, cached)

	// Views of one schema don't answer for another schema with the same key.
	_, found, cached = c.GetCachedViewDefinition(key, "s2", "v")
	assert.False(t, found)
	assert.False(t, cached)

	_, found, cached = c.GetCachedViewDefinition(otherKey, "s1", "v")
	assert.False(t, found)
	assert.False(t, cached)

	// A schema without views is cached too.
	c.CacheViews(key, nil, "s2")
	_, found, cached = c.GetCachedViewDefinition(key, "s2", "v")
	assert.False(t, found)
	assert.True(t, cached)
}
