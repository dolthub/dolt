This is a performance test rig for testing encoding/decoding of math/big.Float numbers. 

Initial silly test is string encoding vs. binary serialization:

For small numbers (<1m), the space saved by writing numbers as strings is pretty large.
Binary is faster no matter the size of the numbers.


As of June 3rd, these are the numbers I get on a MacBook Pro 2.7 GHz Intel Core i5.

$ ./run.sh 

small numbers save a lot of space as strings
encoding: string from: 0 iterations: 100000
IO  489 kB (100,000 nums) in 11.206898552s (44 kB/s)
encoding: binary from: 0 iterations: 100000
IO  1.2 MB (100,000 nums) in 139.831671ms (8.6 MB/s)

once the numbers are larger than 1m, the space savings are gone
encoding: string from: 900000 to: 1000000 by: 1
IO  600 kB (100,000 nums) in 11.185759014s (54 kB/s)
encoding: string from: 1000000 to: 1100000 by: 1
IO  1.2 MB (100,000 nums) in 11.19562483s (106 kB/s)
encoding: binary from: 900000 to: 1000000 by: 1
IO  1.2 MB (100,000 nums) in 128.295162ms (9.4 MB/s)
encoding: binary from: 1000000 to: 1100000 by: 1
IO  1.2 MB (100,000 nums) in 132.097144ms (9.1 MB/s)

binary is always faster
encoding: string from: 0 to: 1000000 by: 2
IO  2.9 MB (500,000 nums) in 56.650322934s (52 kB/s)
encoding: binary from: 0 to: 1000000 by: 2
IO  6.0 MB (500,000 nums) in 718.012386ms (8.4 MB/s)

larger numbers are the same - binary still faster, but now strings are bigger than binary
encoding: string from: 1000000000000 to: 1000000001000 by: 2
IO  8.9 kB (500 nums) in 58.713475ms (151 kB/s)
encoding: binary from: 1000000000000 to: 1000000001000 by: 2
IO  6.0 kB (500 nums) in 877.172µs (6.8 MB/s)

if we special case non-floating numbers we get some space savings
encoding: binary from: 0 to: 10000000 by: 1
IO  120 MB (10,000,000 nums) in 14.025595909s (8.6 MB/s)
encoding: binary-int from: 0 to: 10000000 by: 1
IO  90 MB (10,000,000 nums) in 11.259321946s (8.0 MB/s)

then if we special case small integers we get even more space savings
encoding: binary-varint from: 0 to: 10000000 by: 1
IO  49 MB (10,000,000 nums) in 10.096619263s (4.8 MB/s)

TODO:
- Implement flag for setting precision
- Implement randomized numbers rather than in sequence incremental numbers
- Look at go package "encoding/gob", optimized for performance
- Implement read/write to/from file to connect to JS implementation