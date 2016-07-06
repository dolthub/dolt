// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"io"
	"math/big"
)

const (
	ENCODER_DECODER_PREC = 1024
)

type EncoderDecoder interface {
	Encode(w io.Writer, n *big.Float) error // write n to w
	Decode(r io.Reader, n *big.Float) error // read from r to set n
}
