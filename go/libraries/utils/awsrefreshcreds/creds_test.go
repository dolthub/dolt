// Copyright 2023 Dolthub, Inc.
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

package awsrefreshcreds

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticProvider struct {
	v credentials.Value
}

func (p *staticProvider) Retrieve() (credentials.Value, error) {
	return p.v, nil
}

func (p *staticProvider) IsExpired() bool {
	return false
}

func TestRefreshingCredentialsProvider(t *testing.T) {
	var sp staticProvider
	sp.v.AccessKeyID = "ExampleOne"
	rp := NewRefreshingCredentialsProvider(&sp, time.Minute)

	n := time.Now()
	origNow := now
	t.Cleanup(func() {
		now = origNow
	})
	now = func() time.Time { return n }

	v, err := rp.Retrieve()
	assert.NoError(t, err)
	assert.Equal(t, "ExampleOne", v.AccessKeyID)
	assert.False(t, rp.IsExpired())

	sp.v.AccessKeyID = "ExampleTwo"

	now = func() time.Time { return n.Add(30 * time.Second) }

	v, err = rp.Retrieve()
	assert.NoError(t, err)
	assert.Equal(t, "ExampleTwo", v.AccessKeyID)
	assert.False(t, rp.IsExpired())

	now = func() time.Time { return n.Add(91 * time.Second) }
	assert.True(t, rp.IsExpired())
	v, err = rp.Retrieve()
	assert.NoError(t, err)
	assert.Equal(t, "ExampleTwo", v.AccessKeyID)
	assert.False(t, rp.IsExpired())
}

func TestRefreshingCredentialsProviderShared(t *testing.T) {
	d := t.TempDir()

	onecontents := `
[backup]
aws_access_key_id = AKIAAAAAAAAAAAAAAAAA
aws_secret_access_key = oF8x/JQEGchAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
`

	twocontents := `
[backup]
aws_access_key_id = AKIZZZZZZZZZZZZZZZZZ
aws_secret_access_key = oF8x/JQEGchZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ
`

	configpath := filepath.Join(d, "config")

	require.NoError(t, os.WriteFile(configpath, []byte(onecontents), 0700))

	n := time.Now()
	origNow := now
	t.Cleanup(func() {
		now = origNow
	})
	now = func() time.Time { return n }

	creds := credentials.NewCredentials(
		NewRefreshingCredentialsProvider(&credentials.SharedCredentialsProvider{
			Filename: configpath,
			Profile:  "backup",
		}, time.Minute),
	)

	v, err := creds.Get()
	assert.NoError(t, err)
	assert.Equal(t, "AKIAAAAAAAAAAAAAAAAA", v.AccessKeyID)

	require.NoError(t, os.WriteFile(configpath, []byte(twocontents), 0700))

	now = func() time.Time { return n.Add(61 * time.Second) }
	v, err = creds.Get()
	assert.NoError(t, err)
	assert.Equal(t, "AKIZZZZZZZZZZZZZZZZZ", v.AccessKeyID)
}
