// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/attic-labs/noms/go/types"
)

// KindSlice is an alias for []types.NomsKind. It's needed because types.NomsKind are really just 8 bit unsigned ints, which are what Go uses to represent 'byte', and this confuses the Go JSON marshal/unmarshal code --  it treats them as byte arrays and base64 encodes them!
type KindSlice []types.NomsKind

func (ks KindSlice) MarshalJSON() ([]byte, error) {
	elems := make([]string, len(ks))
	for i, k := range ks {
		elems[i] = fmt.Sprintf("%d", k)
	}
	return []byte("[" + strings.Join(elems, ",") + "]"), nil
}

func (ks *KindSlice) UnmarshalJSON(value []byte) error {
	elems := strings.Split(string(value[1:len(value)-1]), ",")
	*ks = make(KindSlice, len(elems))
	for i, e := range elems {
		ival, err := strconv.ParseUint(e, 10, 8)
		if err != nil {
			return err
		}
		(*ks)[i] = types.NomsKind(ival)
	}
	return nil
}
