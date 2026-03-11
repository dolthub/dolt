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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package spec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

func TestAbsolutePathToAndFromString(t *testing.T) {
	assert := assert.New(t)

	test := func(str string) {
		p, err := NewAbsolutePath(str)
		assert.NoError(err)
		assert.Equal(str, p.String())
	}

	h, err := types.Float(42).Hash(types.Format_Default) // arbitrary hash
	assert.NoError(err)
	test("/refs/heads/main")
	test(fmt.Sprintf("#%s", h.String()))
}

func TestAbsolutePathParseErrors(t *testing.T) {
	test := func(path, errMsg string) {
		p, err := NewAbsolutePath(path)
		assert.Equal(t, AbsolutePath{}, p)
		require.Error(t, err)
		assert.Equal(t, errMsg, err.Error())
	}

	test("", "empty path")
	test("#", "invalid hash: ")
	test("#abc", "invalid hash: abc")
	invHash := strings.Repeat("z", hash.StringLen)
	test("#"+invHash, "invalid hash: "+invHash)
}
