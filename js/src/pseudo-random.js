// @flow

// Linear Congruential Generator
// Variant of a Lehman Generator

// Set to values from http://en.wikipedia.org/wiki/Numerical_Recipes
// m is basically chosen to be large (as it is the max period)
// and for its relationships to a and c
const m = 4294967296;
// a - 1 should be divisible by m's prime factors
const a = 1664525;
// c and m should be co-prime
const c = 1013904223;

export default class Random {
  _z: number;
  constructor(seed: number) {
    this._z = seed;
  }

  nextUint8(): number {
    this._z = (a * this._z + c) % m;
    return this._z & 0xff;
  }
}
