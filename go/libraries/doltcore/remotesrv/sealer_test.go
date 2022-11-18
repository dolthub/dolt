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

package remotesrv

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSingleSymmetricKeySealer(t *testing.T) {
	s, err := NewSingleSymmetricKeySealer()
	assert.NoError(t, err)
	assert.NotNil(t, s)

	u := &url.URL{
		Scheme: "https",
		Host:   "remotesapi.dolthub.com:443",
		Path:   "somedatabasename/sometablefilename",
	}
	sealed, err := s.Seal(u)
	assert.NoError(t, err)
	unsealed, err := s.Unseal(sealed)
	assert.NoError(t, err)
	assert.Equal(t, u, unsealed)

	corruptednbf := &(*sealed)
	ps := corruptednbf.Query()
	ps.Set("nbf", fmt.Sprintf("%v", time.Now()))
	corruptednbf.RawQuery = ps.Encode()
	unsealed, err = s.Unseal(corruptednbf)
	assert.Error(t, err)

	nonbf := &(*sealed)
	ps = nonbf.Query()
	ps.Del("nbf")
	nonbf.RawQuery = ps.Encode()
	unsealed, err = s.Unseal(nonbf)
	assert.Error(t, err)

	corruptedexp := &(*sealed)
	ps = corruptedexp.Query()
	ps.Set("exp", fmt.Sprintf("%v", time.Now()))
	corruptedexp.RawQuery = ps.Encode()
	unsealed, err = s.Unseal(corruptedexp)
	assert.Error(t, err)

	noexp := &(*sealed)
	ps = noexp.Query()
	ps.Del("exp")
	noexp.RawQuery = ps.Encode()
	unsealed, err = s.Unseal(noexp)
	assert.Error(t, err)

	corruptednonce := &(*sealed)
	ps = corruptednonce.Query()
	var differentnonce [12]byte
	_, err = rand.Read(differentnonce[:])
	assert.NoError(t, err)
	ps.Set("nonce", base64.RawURLEncoding.EncodeToString(differentnonce[:]))
	corruptednonce.RawQuery = ps.Encode()
	unsealed, err = s.Unseal(corruptednonce)
	assert.Error(t, err)

	nononce := &(*sealed)
	ps = nononce.Query()
	ps.Del("nonce")
	nononce.RawQuery = ps.Encode()
	unsealed, err = s.Unseal(nononce)
	assert.Error(t, err)
}
