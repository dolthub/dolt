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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnsureSuffix(t *testing.T) {
	type args struct {
		s      string
		suffix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"adds a suffix when not present", args{"foo", ".bar"}, "foo.bar"},
		{"doesn't add a suffix when already present", args{"foo.bar", ".bar"}, "foo.bar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EnsureSuffix(tt.args.s, tt.args.suffix)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLastSegment(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"gets the last segment of a slash-separated string", args{"foo/bar/baz"}, "baz"},
		{"gets the name at the end of a path", args{"/Users/foouser/some/path/somewhere"}, "somewhere"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LastSegment(tt.args.s)
			assert.Equal(t, tt.want, got)
		})
	}
}
