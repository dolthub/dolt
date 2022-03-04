// Copyright 2022 Dolthub, Inc.
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

package datas

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestRefMap(t *testing.T) {
	var rm refmap
	rm.set("refs/heads/main", hash.Parse("dhuvd5ujhsndlqrbds90vapt2325v7lq"))
	rm.set("refs/heads/branch", hash.Parse("vrgo3ao6fcqrsr6chqlakanqeg936i9c"))
	got := rm.lookup("refs/heads/main")
	assert.Equal(t, hash.Parse("dhuvd5ujhsndlqrbds90vapt2325v7lq"), got)
	got = rm.lookup("nonexistant")
	assert.Equal(t, hash.Hash{}, got)
	serialized := rm.storeroot_flatbuffer()
	assert.NotNil(t, serialized)
	parsed := parse_storeroot(serialized)
	assert.Len(t, parsed.entries, 2)
	got = parsed.lookup("refs/heads/main")
	assert.Equal(t, hash.Parse("dhuvd5ujhsndlqrbds90vapt2325v7lq"), got)
	got = parsed.lookup("refs/heads/branch")
	assert.Equal(t, hash.Parse("vrgo3ao6fcqrsr6chqlakanqeg936i9c"), got)
	got = parsed.lookup("nonexistant")
	assert.Equal(t, hash.Hash{}, got)
}

func TestRefMapDelete(t *testing.T) {
	var rm refmap
	rm.set("refs/heads/main", hash.Parse("dhuvd5ujhsndlqrbds90vapt2325v7lq"))
	rm.set("refs/heads/branch", hash.Parse("vrgo3ao6fcqrsr6chqlakanqeg936i9c"))
	rm.delete("refs/heads/main")
	got := rm.lookup("refs/heads/branch")
	assert.Equal(t, hash.Parse("vrgo3ao6fcqrsr6chqlakanqeg936i9c"), got)
	got = rm.lookup("refs/heads/main")
	assert.Equal(t, hash.Hash{}, got)
}
