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
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ini "github.com/dolthub/aws-sdk-go-ini-parser"
)

var now func() time.Time = time.Now

var _ aws.CredentialsProvider = (*RefreshingCredentialsProvider)(nil)

type RefreshingCredentialsProvider struct {
	provider aws.CredentialsProvider
	interval time.Duration
}

func NewRefreshingCredentialsProvider(provider aws.CredentialsProvider, interval time.Duration) *RefreshingCredentialsProvider {
	return &RefreshingCredentialsProvider{
		provider: provider,
		interval: interval,
	}
}

func (p *RefreshingCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	res, err := p.provider.Retrieve(ctx)
	if err == nil && res.CanExpire == false {
		res.CanExpire = true
		res.Expires = now().Add(p.interval)
	}
	return res, err
}

// Based on the behavior of EnvConfig in aws-sdk-go-v2.
func LoadEnvCredentials() aws.Credentials {
	var ret aws.Credentials
	ret.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	if ret.AccessKeyID == "" {
		ret.AccessKeyID = os.Getenv("AWS_ACCESS_KEY")
	}
	ret.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	if ret.SecretAccessKey == "" {
		ret.SecretAccessKey = os.Getenv("AWS_SECRET_KEY")
	}
	if ret.HasKeys() {
		ret.SessionToken = os.Getenv("AWS_SESSION_TOKEN")
		ret.Source = "EnvironmentVariables"
		return ret
	}
	return aws.Credentials{}
}

func LoadINICredentialsProvider(filename, profile string) aws.CredentialsProvider {
	if profile == "" {
		profile = os.Getenv("AWS_PROFILE")
	}
	if profile == "" {
		profile = "default"
	}
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		sections, err := ini.OpenFile(filename)
		if err != nil {
			return aws.Credentials{}, err
		}
		section, ok := sections.GetSection(profile)
		if !ok {
			return aws.Credentials{}, fmt.Errorf("error loading credentials for profile %s from file %s; profile not found", profile, filename)
		}

		id := section.String("aws_access_key_id")
		if len(id) == 0 {
			return aws.Credentials{}, fmt.Errorf("error loading credentials for profile %s from file %s; no aws_access_key_id", profile, filename)
		}

		secret := section.String("aws_secret_access_key")
		if len(secret) == 0 {
			return aws.Credentials{}, fmt.Errorf("error loading credentials for profile %s from file %s; no aws_secret_access_key", profile, filename)
		}

		// Default to empty string if not found
		token := section.String("aws_session_token")

		return aws.Credentials{
			AccessKeyID:     id,
			SecretAccessKey: secret,
			SessionToken:    token,
			Source:          "SharedCredentialsFile",
		}, nil
	})
}
