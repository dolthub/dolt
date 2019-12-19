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
	"log"

	flag "github.com/juju/gnuflag"
)

var (
	verbose bool
	quiet   bool
)

var LogFunc func(ctx context.Context, format string, args ...interface{})

func init() {
	LogFunc = func(ctx context.Context, format string, args ...interface{}) {
		if len(args) > 0 {
			log.Printf(format+"\n", args...)
		} else {
			log.Println(format)
		}
	}
}

// RegisterVerboseFlags registers -v|--verbose flags for general usage
func RegisterVerboseFlags(flags *flag.FlagSet) {
	flags.BoolVar(&verbose, "verbose", false, "show more")
	flags.BoolVar(&verbose, "v", false, "")
}

// Verbose returns True if the verbose flag was set
func Verbose() bool {
	return verbose
}

func SetVerbose(v bool) {
	verbose = v
}

// Log calls Printf(format, args...) iff Verbose() returns true.
func Log(ctx context.Context, format string, args ...interface{}) {
	if Verbose() {
		LogFunc(ctx, format, args)
	}
}
