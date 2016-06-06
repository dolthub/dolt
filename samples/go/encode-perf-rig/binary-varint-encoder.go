// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"encoding/binary"
	"io"
	"math/big"
)

type BinaryVarintEncoderDecoder struct {
	tmp *big.Float
}

func NewBinaryVarintEncoderDecoder() BinaryVarintEncoderDecoder {
	bed := BinaryVarintEncoderDecoder{}
	bed.tmp = new(big.Float)
	bed.tmp.SetPrec(ENCODER_DECODER_PREC)
	return bed
}

func (bed BinaryVarintEncoderDecoder) Encode(w io.Writer, n *big.Float) error {
	if n.IsInt() {
		x, _ := n.Int64()
		// TODO - if accuracy is not Exact, then use the other path
		buf := make([]byte, binary.MaxVarintLen64)
		nBytes := binary.PutVarint(buf, x)
		if _, err := w.Write([]byte{byte(0)}); err != nil {
			return err
		}
		_, err := w.Write(buf[0:nBytes])
		return err
	} else {
		if err := binary.Write(w, binary.BigEndian, int8(1)); err != nil {
			return err
		}

		exponent := n.MantExp(bed.tmp)
		f, _ := bed.tmp.Float64()
		if err := binary.Write(w, binary.BigEndian, f); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, int32(exponent))
	}
}

func (bed BinaryVarintEncoderDecoder) Decode(r io.Reader, n *big.Float) error {
	var isInteger int8
	var f float64
	var exponent int32
	n.SetUint64(0)

	if err := binary.Read(r, binary.BigEndian, &isInteger); err != nil {
		return err
	}

	if isInteger <= 0 {
		var x int64
		var err error
		if x, err = binary.ReadVarint(miniByteReader{r}); err != nil {
			return err
		}
		n.SetInt64(x)
		n.SetPrec(ENCODER_DECODER_PREC)
		return nil
	} else {
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
}

type miniByteReader struct {
	r io.Reader
}

func (r miniByteReader) ReadByte() (byte, error) {
	p := []byte{byte(0)}
	// io.Reader.Read(p []byte) (n int, err error)
	// io.ByteReader.ReadByte() (c byte, err error)
	_, err := r.r.Read(p)
	return p[0], err
}
