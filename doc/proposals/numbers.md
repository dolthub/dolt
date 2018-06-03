# It's time to fix numbers in Noms

Currently numbers in Noms are are stored as arbitrary precision decimal numbers.

However, the API represents numbers as `float64`, so it is not possible to store even 64-bit integers in Noms accurately today.

The last time we looked at this, we concluded we didn't have a clear enough idea of the desired properties for numbers, so we decided to wait for more information.

It's now clear to me that desiderata include:

## Use cases

* It should be possible to store arbitrarily large integers in Noms
* It should be possible to store arbitrarily precise non-integral numbers in Noms
* It should be possible to store rational, non-integral numbers precisely in Noms

## Integration

* The Noms type system should report useful information about numbers:
  * Whether the number can be represented precisely in binary
  * The number of bits required to represent a number precisely
  * Whether the number is signed
* It must remain the case that every unique numeric value in the system has one (and only one) encoding and hash
  * This implies that the value `uint64(42)` is not possible in Noms. The type of `42` is always `uint8`.
* It should be possible to use native Go numeric types like `int64`, `float`, and `big.Rat` to work with Noms numbers in the cases they fit
* It should be possible to conveniently work with _all_ Noms values via some consistent interface if so desired

## Efficiency

* It should be as close as possible to zero work to decode and encode all the common numeric types
* Large, imprecise numbers (e.g., 2^1000) should be stored compactly so that users don't have to manually mess with scale to try and save space

## Non-goals

* I do not think a database system like Noms should support fixed-size (e.g., floating point) fractional values.
* I do not care if it is possible to represent every possible IEEE float (e.g., NaN, Infinity, etc)

# Proposal for type system integration

```
// All Numbers in Noms are represented with a single type.
// Binary numbers have the form: np * 2^ne, where:
// - np: signed integer
// - ne: unsigned integer
// - ne can be less than np, in which case the number is non-integral
// Rational numbers have the form: (np * 2^ne) / (dp * 2^de), where np and ne have same meaning as binary numbers, and:
// - dp, de: unsigned integers
// - ne >= np and de >= dp
Number<signed, numprec, numexp[, denprec, denexp]>

// For user convenience, when we print number types, we will simplify them by default:
Number<Signed, 6, 6> -> uint8
Number<Unsigned, 14, 4> -> float32
Number<Unsigned, 2, 100> -> bigint<unsigned, 2, 100>
Number<Signed, 100, 2> -> bigfloat<signed, 100, 2>
Number<Signed, 1, 1, 2, 2> -> rational<signed, 1, 2>

Set<42, 88.8, -17>.Type() -> float32 (but internally we know that it is Number<Signed, 10, 7>)

// FUTURE: We could also optionally support the opposite -> specifying shorthand types and interpreting them internally as the long form
```

# Type accretion support

Given above, we can implement correct type accretion:

```
A=accrete(...type)
n=Number

A(42, 7) => Number<Unsigned, 6, 6> or "uint8"
A(42, -42) => Number<Signed, 6, 6> or "int8"
A(42, 88.8) => Number<Unsigned, 10, 7> or "float32"
A(2^64, 1/(2^64) => Number<Unsigned, 64, 64> or "bigfloat<signed, 64, 64>" (note: *not* float64!)
... etc ...
```

Implementing type accretion is why it is important for types to carry information about number of bits required for both precision and exponent.

# Serialization

Just because we have one Noms type doesn't mean we need one uniform serialization. In order to achieve our goal of zero copy encode/decode being possible, we will use the common encodings of standard numeric types.

* For numbers that fit in standard `(u)int(8|16|32|64)` just encode them that way (little-endian).
* For numbers that fit in `float(32|64)` without loss of precision, canonicaliz them and store as standard floats.
* For other numbers, we will do a custom encoding, likely a variant of floating point but with arbitrary size for binary numbers and the same but with the denominator too for rational numbers.

# Other notes

* You should be able to construct numbers out of any native Go numeric type, including `big.*`
* We won't support NaN, infinity, negative zero, or other odd values
