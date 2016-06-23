// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package hash

import "encoding/base32"

var encoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv")

func encode(data []byte) string {
	return encoding.EncodeToString(data[:20]) // We only use the first remove trailing padding
}

func decode(s string) []byte {
	slice, _ := encoding.DecodeString(s)
	return slice
}
