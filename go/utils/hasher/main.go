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

package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/dolthub/dolt/go/store/hash"
)

// hasher is a simple utility for converting between dolt base32 hashes and raw bytes. If you give it one argument,
// the assumption is that it's a hash:
//
// $ hasher 201orjntip8jb6annkfsmiue9h4309k9
// [16 3 141 206 253 150 81 53 153 87 189 31 203 75 206 76 72 48 38 137]
//
// If you give it multiple arguments, the assumption is that they're bytes. if you only specify a few bytes, the
// tail will be 0s:
//
// $ hasher 16 3 141 206 253 150
// 201orjnt000000000000000000000000
//
// Why? When you are looking at a byte array in the debugger and you need to know if it's a specific hash this
// can help. base32 conversion in your head no longer required, which is what aaron always does.

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Usage: hasher <hash> OR hasher <uint8> <uint8> ...")
		return
	}

	if len(os.Args) == 2 {
		hashStr := os.Args[1]
		if !hash.IsValid(hashStr) {
			fmt.Println("Invalid hash")
			return
		}
		h := hash.Parse(hashStr)
		fmt.Printf("%v\n", h[:])
	} else {
		var raw hash.Hash

		bytesGiven := len(os.Args) - 1

		if bytesGiven > hash.ByteLen {
			fmt.Println("Too many bytes given.")
			return
		}

		for i := 1; i < bytesGiven; i++ {
			val, err := strconv.ParseUint(os.Args[i], 10, 8)
			if err != nil {
				fmt.Println(err)
				return
			}
			raw[i-1] = uint8(val)
		}

		fmt.Printf("%v\n", raw)
	}
}
