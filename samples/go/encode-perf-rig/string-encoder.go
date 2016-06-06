// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"io"
	"math/big"
)

type StringEncoderDecoder struct{}

func NewStringEncodedDecoder() StringEncoderDecoder {
	return StringEncoderDecoder{}
}

func (sed StringEncoderDecoder) Encode(w io.Writer, n *big.Float) error {
	// TODO - big.Float.MarshalText?
	// TODO - big.Float.Append
	str := []byte(n.Text('g', -1))
	_, err := w.Write(str)
	return err
}

func (sed StringEncoderDecoder) Decode(r io.Reader, n *big.Float) error {
	n.SetFloat64(0)
	n.SetPrec(ENCODER_DECODER_PREC)

	buf := make([]byte, 256)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	_, _, err := n.Parse(string(buf), 10)
	return err
}
