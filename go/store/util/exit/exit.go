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

// Package exit provides a mockable implementation of os.Exit.
// That's all!
package exit

import (
	"os"
)

var def = func(code int) {
	os.Exit(code)
}

var Exit = def

// Reset sets the implementation of Exit() to the default.
func Reset() {
	Exit = def
}

// Fail exits with a failure status.
func Fail() {
	Exit(1)
}

// Success exits with a success status.
func Success() {
	Exit(0)
}
