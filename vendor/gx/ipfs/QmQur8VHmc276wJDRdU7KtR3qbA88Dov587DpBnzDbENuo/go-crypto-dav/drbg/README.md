This Go library provides a cryptographically secure pseudorandom number generator.
Specifically, it implements **HMAC_DRBG** (SHA-512) as specified in
[NIST SP 800-90A](http://csrc.nist.gov/publications/nistpubs/800-90A/SP800-90A.pdf).

For simplicity, this library currently does not track the seed period,
so the `Read` function always returns the requested number of bytes.
It is the user's responsibility to periodically reseed the PRNG.

This library is tested with NIST-provided test vectors.

**See also**: [python-drbg](https://github.com/davidlazar/python-drbg).
