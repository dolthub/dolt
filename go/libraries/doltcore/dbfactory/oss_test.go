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

package dbfactory

import (
	"os"
	"testing"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/stretchr/testify/assert"
)

func Test_readOssCredentialsFromFile(t *testing.T) {
	creds, err := readOSSCredentialsFromFile("testdata/osscred/dolt_oss_credentials")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(creds))
}

func Test_ossConfigFromParams(t *testing.T) {
	type args struct {
		params map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want ossCredential
	}{
		{
			name: "not exist",
			args: args{
				params: nil,
			},
			want: emptyOSSCredential,
		},
		{
			name: "get default profile",
			args: args{
				params: map[string]interface{}{
					OSSCredsFileParam: "testdata/osscred/dolt_oss_credentials",
				},
			},
			want: ossCredential{
				Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
				AccessKeyID:     "defaulttestid",
				AccessKeySecret: "test secret",
			},
		},
		{
			name: "get default profile single cred",
			args: args{
				params: map[string]interface{}{
					OSSCredsFileParam: "testdata/osscred/single_oss_cred",
				},
			},
			want: ossCredential{
				Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
				AccessKeyID:     "testid",
				AccessKeySecret: "test secret",
			},
		},
		{
			name: "get cred by profile",
			args: args{
				params: map[string]interface{}{
					OSSCredsFileParam: "testdata/osscred/dolt_oss_credentials",
					OSSCredsProfile:   "prod",
				},
			},
			want: ossCredential{
				Endpoint:        "oss-cn-hangzhou.aliyuncs.com",
				AccessKeyID:     "prodid",
				AccessKeySecret: "test secret",
			},
		},
		{
			name: "profile not exists",
			args: args{
				params: map[string]interface{}{
					OSSCredsFileParam: "testdata/osscred/dolt_oss_credentials",
					OSSCredsProfile:   "notexists",
				},
			},
			want: emptyOSSCredential,
		},
		{
			name: "empty cred file",
			args: args{
				params: map[string]interface{}{
					OSSCredsFileParam: "testdata/osscred/empty_oss_cred",
				},
			},
			want: emptyOSSCredential,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, ossConfigFromParams(tt.args.params), "ossConfigFromParams(%v)", tt.args.params)
		})
	}
}

func Test_getOSSClient(t *testing.T) {
	type args struct {
		opts ossCredential
	}
	tests := []struct {
		name    string
		args    args
		before  func()
		after   func()
		want    func(got *oss.Client) bool
		wantErr bool
	}{
		{
			name: "get valid oss client",
			args: args{
				opts: ossCredential{
					Endpoint:        "testendpoint",
					AccessKeyID:     "testid",
					AccessKeySecret: "testkey",
				},
			},
			wantErr: false,
			want: func(got *oss.Client) bool {
				return got != nil
			},
		},
		{
			name: "get invalid oss client",
			args: args{
				opts: ossCredential{
					Endpoint:        "",
					AccessKeyID:     "testid",
					AccessKeySecret: "testkey",
				},
			},
			wantErr: true,
			want: func(got *oss.Client) bool {
				return got == nil
			},
		},
		{
			name: "get valid oss client from env",
			before: func() {
				os.Setenv(dconfig.EnvOssEndpoint, "testendpoint")
			},
			after: func() {
				os.Unsetenv(dconfig.EnvOssEndpoint)
			},
			args: args{
				opts: ossCredential{
					Endpoint:        "",
					AccessKeyID:     "testid",
					AccessKeySecret: "testkey",
				},
			},
			wantErr: false,
			want: func(got *oss.Client) bool {
				return got != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.after != nil {
				defer tt.after()
			}
			got, err := getOSSClient(tt.args.opts)
			if tt.wantErr {
				assert.Error(t, err)
			}
			assert.True(t, tt.want(got))
		})
	}
}
