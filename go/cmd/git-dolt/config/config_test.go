// Copyright 2019 Liquidata, Inc.
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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const version = "0.0.0"
const remoteURL = "http://localhost:50051/test-org/test-repo"
const revision = "nl5v5qu36e2dfmhnjqiu4crefam52iif"

var testConfig = fmt.Sprintf(`version %s
remote %s
revision %s
`, version, remoteURL, revision)

var noVersionConfig = fmt.Sprintf(`remote %s
revision %s
`, remoteURL, revision)

var noRemoteConfig = fmt.Sprintf(`version %s
revision %s
`, version, revision)

var noRevisionConfig = fmt.Sprintf(`version %s
remote %s
`, version, remoteURL)

var wanted = GitDoltConfig{
	Version:  version,
	Remote:   remoteURL,
	Revision: revision,
}

func TestParse(t *testing.T) {
	type args struct {
		c string
	}
	happyTests := []struct {
		name string
		args args
		want GitDoltConfig
	}{
		{"parses config", args{testConfig}, wanted},
		{"defaults version to current git-dolt version if missing", args{noVersionConfig}, wanted},
	}
	for _, tt := range happyTests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args.c)
			assert.Nil(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	errorTests := []struct {
		name string
		args args
		want error
	}{
		{"returns an error if missing remote", args{noRemoteConfig}, errors.New("no remote specified")},
		{"returns an error if missing revision", args{noRevisionConfig}, errors.New("no revision specified")},
	}
	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.args.c)
			assert.Equal(t, tt.want, err)
		})
	}
}
