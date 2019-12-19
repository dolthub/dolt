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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package verbose

import (
	"context"
	"os"

	flag "github.com/juju/gnuflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	verbose bool
	quiet   bool
)

// RegisterVerboseFlags registers -v|--verbose flags for general usage
func RegisterVerboseFlags(flags *flag.FlagSet) {
	flags.BoolVar(&verbose, "verbose", false, "show more")
	flags.BoolVar(&verbose, "v", false, "")
}

func SetVerbose(v bool) {
	verbose = v
}

// A function which will be called for logging throughout the doltcore/store
// layer. Defaults to logging to STDERR at Debug level if --verbose is set and
// Warn level if --verbose is not set.
//
// May be called with `nil` context in non-context-aware functions.
var Logger func(ctx context.Context) *zap.Logger

func init() {
	enabler := zap.LevelEnablerFunc(func(l zapcore.Level) bool {
		if verbose {
			return zapcore.DebugLevel.Enabled(l)
		} else {
			return zapcore.WarnLevel.Enabled(l)
		}
	})
	encoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
	core := zapcore.NewCore(encoder, zapcore.Lock(os.Stderr), enabler)
	Logger = func(ctx context.Context) *zap.Logger {
		return zap.New(core)
	}
}
