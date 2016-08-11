// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"time"

	humanize "github.com/dustin/go-humanize"
	flag "github.com/juju/gnuflag"
)

// used to ensure all of the big.Floats end up with the same precision
func newBigFloat(n uint64) *big.Float {
	tmp := new(big.Float).SetUint64(n)
	tmp.SetPrec(ENCODER_DECODER_PREC)
	return tmp
}

// helpful in debugging output
func stringOfBigFloat(n *big.Float) string {
	return n.Text('g', -1)
}

func runTest(encoderDecoder EncoderDecoder, n *big.Float) (nBytes uint64) {
	y := newBigFloat(0)

	buf := new(bytes.Buffer)
	err := encoderDecoder.Encode(buf, n)
	if err != nil {
		panic(err)
	}
	nBytes += uint64(buf.Len())
	buf = bytes.NewBuffer(buf.Bytes())

	err = encoderDecoder.Decode(buf, y)
	nBytes += uint64(buf.Len())
	if n.Cmp(y) != 0 {
		panic(fmt.Sprintf("write and read are not the same: %v, %v - %d - %d, %d", stringOfBigFloat(n), stringOfBigFloat(y), nBytes, n.Prec(), y.Prec()))
	}
	return
}

func getEncoder(name string) EncoderDecoder {
	if name == "string" {
		return StringEncodedDecoder()
	} else if name == "binary" {
		return NewBinaryEncoderDecoder()
	} else if name == "binary-int" {
		return NewBinaryIntEncoderDecoder()
	} else if name == "binary-varint" {
		return NewBinaryVarintEncoderDecoder()
	} else {
		fmt.Printf("Unknown encoding specified: %s\n", name)
		flag.PrintDefaults()
		os.Exit(1)
		return nil
	}
}

func main() {
	// use [from/to/by] or [from/iterations]
	nFrom := flag.Uint64("from", 1e2, "start iterations from this number")
	nTo := flag.Uint64("to", 1e4, "run iterations until arriving at this number")
	nBy := flag.Uint64("by", 1, "increment each iteration by this number")
	nIncrements := flag.Uint64("iterations", 0, "number of iterations to execute")
	encodingType := flag.String("encoding", "string", "encode/decode as 'string', 'binary', 'binary-int', 'binary-varint'")
	flag.Parse(true)

	flag.Usage = func() {
		fmt.Printf("%s\n", os.Args[0])
		flag.PrintDefaults()
		return
	}

	t0 := time.Now()
	nBytes := uint64(0)
	nIterations := uint64(0)

	encoderDecoder := getEncoder(*encodingType)
	startingLoop := newBigFloat(*nFrom)

	var endLoop *big.Float
	var incrementer *big.Float

	if *nIncrements > 0 {
		// using from/iterations flags
		fmt.Printf("encoding: %v from: %v iterations: %v\n", *encodingType, *nFrom, *nIncrements)

		incrementer = newBigFloat(1)
		n := newBigFloat(*nIncrements)
		endLoop = n.Add(n, startingLoop)
	} else {
		// using from/to/by flags
		fmt.Printf("encoding: %v from: %v to: %v by: %v\n", *encodingType, *nFrom, *nTo, *nBy)
		incrementer = newBigFloat(*nBy)
		endLoop = newBigFloat(*nTo)
	}

	for i := startingLoop; i.Cmp(endLoop) < 0; i = i.Add(i, incrementer) {
		nIterations++
		nBytes += runTest(encoderDecoder, i)
	}

	t1 := time.Now()
	d := t1.Sub(t0)
	fmt.Printf("IO  %s (%v nums) in %s (%s/s)\n", humanize.Bytes(nBytes), humanize.Comma(int64(nIterations)), d, humanize.Bytes(uint64(float64(nBytes)/d.Seconds())))
}
