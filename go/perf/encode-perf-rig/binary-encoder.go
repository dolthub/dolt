// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"encoding/binary"
	"io"
	"math/big"
)

type BinaryEncoderDecoder struct {
	tmp *big.Float
}

func NewBinaryEncoderDecoder() BinaryEncoderDecoder {
	bed := BinaryEncoderDecoder{}
	bed.tmp = new(big.Float)
	bed.tmp.SetPrec(ENCODER_DECODER_PREC)
	return bed
}

func (bed BinaryEncoderDecoder) Encode(w io.Writer, n *big.Float) error {
	exponent := n.MantExp(bed.tmp)
	f, _ := bed.tmp.Float64()

	if err := binary.Write(w, binary.BigEndian, f); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, int32(exponent))
}

func (bed BinaryEncoderDecoder) Decode(r io.Reader, n *big.Float) error {
	var f float64
	var exponent int32
	n.SetUint64(0)
	if err := binary.Read(r, binary.BigEndian, &f); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &exponent); err != nil {
		return err
	}
	bed.tmp.SetFloat64(f)
	bed.tmp.SetPrec(ENCODER_DECODER_PREC)
	n.SetMantExp(bed.tmp, int(exponent))
	return nil
}
