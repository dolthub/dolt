This is a performance test rig for testing encoding/decoding of numbers. 

Initial silly test is string encoding vs. binary serialization:

For small numbers (<1m), the space saved by writing numbers are strings is pretty large.
Binary is faster no matter the size of the numbers.


As of June 3rd, these are the numbers I get on a MacBook Pro 2.7 GHz Intel Core i5.

$ ./run.sh 

small numbers save a lot of space as strings
enc: string from: 0 to: 100000 by: 1
IO 954.86 KB (100000 nums) (5.69 MB/s) processed...
enc: binary from: 0 to: 100000 by: 1
IO 2.29 MB (100000 nums) (9.66 MB/s) processed...

once the numbers are larger than 1 trillion, the space savings are gone
enc: string from: 999999999000 to: 1000000000000 by: 1
IO 23.44 KB (1000 nums) (2.54 MB/s) processed...
enc: string from: 1000000000000 to: 1000000001000 by: 1
IO 25.39 KB (1000 nums) (2.76 MB/s) processed...
enc: binary from: 999999999000 to: 1000000000000 by: 1
IO 23.44 KB (1000 nums) (1.91 MB/s) processed...
enc: binary from: 1000000000000 to: 1000000001000 by: 1
IO 23.44 KB (1000 nums) (2.08 MB/s) processed...

binary is almost always faster in Go but JS appears differently
enc: string from: 0 to: 1000000 by: 2
IO 5.62 MB (500000 nums) (8.30 MB/s) processed...
enc: binary from: 0 to: 1000000 by: 2
IO 11.44 MB (500000 nums) (10.64 MB/s) processed...

larger numbers - strings are now bigger than binary, but binary is slowing down
enc: string from: 100000000000000 to: 100000000001000 by: 2
IO 14.65 KB (500 nums) (1.59 MB/s) processed...
enc: binary from: 100000000000000 to: 100000000001000 by: 2
IO 11.72 KB (500 nums) (1.04 MB/s) processed...

if we special case non-floating numbers we get some space savings
enc: binary from: 0 to: 10000000 by: 1
IO 228.88 MB (10000000 nums) (11.31 MB/s) processed...
enc: binary-int from: 0 to: 10000000 by: 1
IO 95.37 MB (10000000 nums) (11.47 MB/s) processed...
enc: string from: 0 to: 10000000 by: 1
IO 131.40 MB (10000000 nums) (9.47 MB/s) processed...

then if we special case small integers we get even more space savings
enc: binary-varint from: 0 to: 10000000 by: 1
IO 72.26 MB (10000000 nums) (8.78 MB/s) processed...


TODO:
- Implement randomized numbers rather than in sequence incremental numbers
- Implement using a bignum equivalent npm package
- Implement read/write to/from file to connect to Go implementation