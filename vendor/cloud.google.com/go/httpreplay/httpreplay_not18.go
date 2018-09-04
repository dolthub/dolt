// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !go1.8

// httpreplay is available in go1.8 and forward. This file exists only for 1.6 and 1.7 to
// compile, since every package must have some buildable file else go test ./... fails.
package httpreplay

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// Supported reports whether httpreplay is supported in the current version of Go.
// For Go 1.7 and below, the answer is false.
func Supported() bool { return false }

type (
	Recorder struct{}

	Replayer struct{}
)

func NewRecorder(string, []byte) (*Recorder, error) { return nil, nil }

func (*Recorder) Client(context.Context, ...option.ClientOption) (*http.Client, error) {
	return nil, nil
}
func (*Recorder) Close() error { return nil }

func NewReplayer(string) (*Replayer, error) { return nil, nil }

func (*Replayer) Initial() []byte                              { return nil }
func (*Replayer) IgnoreHeader(string)                          {}
func (*Replayer) Client(context.Context) (*http.Client, error) { return nil, nil }
func (*Replayer) Close() error                                 { return nil }

func DebugHeaders() {}
