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

// Refreshing credentials will periodically refresh credentials from the
// underlying credential provider. This can be used in places where temporary
// credentials are placed into files, for example, and we need profile
// credentials periodically refreshed, for example.

package awsrefreshcreds

import (
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

var now func() time.Time = time.Now

type RefreshingCredentialsProvider struct {
	provider credentials.Provider

	refreshedAt     time.Time
	refreshInterval time.Duration
}

func NewRefreshingCredentialsProvider(provider credentials.Provider, interval time.Duration) *RefreshingCredentialsProvider {
	return &RefreshingCredentialsProvider{
		provider:        provider,
		refreshInterval: interval,
	}
}

func (p *RefreshingCredentialsProvider) Retrieve() (credentials.Value, error) {
	v, err := p.provider.Retrieve()
	if err == nil {
		p.refreshedAt = now()
	}
	return v, err
}

func (p *RefreshingCredentialsProvider) IsExpired() bool {
	if now().Sub(p.refreshedAt) > p.refreshInterval {
		return true
	}
	return p.provider.IsExpired()
}
