// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// http://croquetweak.blogspot.com/2014/08/deconstructing-floats-frexp-and-ldexp.html
// https://github.com/bertfreudenberg/SqueakJS - MIT License
export function frexp(value: number): [number, number] {
  // frexp separates a float into its mantissa and exponent
  if (value === 0) return [value, 0];  // zero is special
  const data = new DataView(new ArrayBuffer(8));
  data.setFloat64(0, value);  // for accessing IEEE-754 exponent bits
  let bits = (data.getUint32(0) >>> 20) & 0x7FF;
  if (bits === 0) {  // we have a subnormal float (actual zero was handled above)
    // make it normal by multiplying a large number
    data.setFloat64(0, value * Math.pow(2, 64));
    // access its exponent bits, and subtract the large number's exponent
    bits = ((data.getUint32(0) >>> 20) & 0x7FF) - 64;
  }
  // apply bias
  const exponent = bits - 1022,
    mantissa = ldexp(value, -exponent);
  return [mantissa, exponent];
}

export function ldexp(mantissa: number, exponent: number): number {
  // construct a float from mantissa and exponent
  return exponent > 1023 // avoid multiplying by infinity
    ? mantissa * Math.pow(2, 1023) * Math.pow(2, exponent - 1023)
    : exponent < -1074 // avoid multiplying by zero
    ? mantissa * Math.pow(2, -1074) * Math.pow(2, exponent + 1074)
    : mantissa * Math.pow(2, exponent);
}
