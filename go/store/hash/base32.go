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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package hash

import "encoding/base32"

var encoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv")

// encode returns the base32 encoding in the Dolt alphabet.
func encode(data []byte) string {
	return encoding.EncodeToString(data)
}

// decode returns the bytes represented by the Base32 string using the Dolt alphabet.
func decode(s string) []byte {
	slice, _ := encoding.DecodeString(s)
	return slice
}
