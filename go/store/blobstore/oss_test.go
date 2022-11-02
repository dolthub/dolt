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

package blobstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_normalizePrefix(t *testing.T) {
	type args struct {
		prefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "no_leading_slash",
			args: args{
				prefix: "root",
			},
			want: "root",
		},
		{
			name: "with_leading_slash",
			args: args{
				prefix: "/root",
			},
			want: "root",
		},
		{
			name: "with_multi_leading_slash",
			args: args{
				prefix: "//root",
			},
			want: "root",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, normalizePrefix(tt.args.prefix), "normalizePrefix(%v)", tt.args.prefix)
		})
	}
}
