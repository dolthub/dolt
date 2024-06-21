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

package ref

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBranchName(t *testing.T) {
	assert.True(t, IsValidBranchName("t"))
	assert.True(t, IsValidBranchName("user/in-progress/do-some-things"))
	assert.True(t, IsValidBranchName("user/in-progress/{}"))
	assert.True(t, IsValidBranchName("user/{/a.tt/}"))
	assert.False(t, IsValidBranchName("☃️"))
	assert.False(t, IsValidBranchName(""))
	assert.False(t, IsValidBranchName("this-is-a-..-test"))
	assert.False(t, IsValidBranchName("this-is-a-@{-test"))
	assert.False(t, IsValidBranchName("this-is-a- -test"))
	assert.False(t, IsValidBranchName("this-is-a-\t-test"))
	assert.False(t, IsValidBranchName("this-is-a-//-test"))
	assert.False(t, IsValidBranchName("this-is-a-:-test"))
	assert.False(t, IsValidBranchName("this-is-a-?-test"))
	assert.False(t, IsValidBranchName("this-is-a-[-test"))
	assert.False(t, IsValidBranchName("this-is-a-\\-test"))
	assert.False(t, IsValidBranchName("this-is-a-^-test"))
	assert.False(t, IsValidBranchName("this-is-a-~-test"))
	assert.False(t, IsValidBranchName("this-is-a-*-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x00-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x01-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x02-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x03-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x04-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x05-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x06-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x07-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x08-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x09-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x0a-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x0b-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x0c-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x0d-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x0e-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x0f-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x10-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x11-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x12-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x13-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x14-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x15-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x16-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x17-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x18-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x19-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x1a-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x1b-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x1c-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x1d-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x1e-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x1f-test"))
	assert.False(t, IsValidBranchName("this-is-a-\x7f-test"))
	assert.False(t, IsValidBranchName("this-is-a-\n-test"))
	assert.False(t, IsValidBranchName("user/working/.tt"))
	assert.False(t, IsValidBranchName(".user/working/a.tt"))
	assert.False(t, IsValidBranchName("user/working/"))
	assert.False(t, IsValidBranchName("/user/working/"))
	assert.False(t, IsValidBranchName("user/working/mybranch.lock"))
	assert.False(t, IsValidBranchName("mybranch.lock"))
	assert.False(t, IsValidBranchName("user.lock/working/mybranch"))
	assert.False(t, IsValidBranchName("HEAD"))
	assert.False(t, IsValidBranchName("-"))
	assert.False(t, IsValidBranchName("-test"))
}
