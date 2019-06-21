This is a performance test rig for the two main types of hashing we do in NOMS - buzhash and sha1. There's also support for sha256, sha512, and blake2b hash functions for comparison.

As of May 9, these are the numbers I get on a macbook pro 3.1 GHz Intel Core i7.

- no hashing    : 3500 MB/s
- sha1 only     :  470 MB/s
- sha256 only   :  185 MB/s
- sha512 only   :  299 MB/s
- blake2b only  :  604 MB/s
- bh only       :  139 MB/s
- sha1 and bh   :  110 MB/s
- sha256 and bh :   80 MB/s
- sha512 and bh :   96 MB/s
- blake2b and bh:  115 MB/s

I think that in the no hashing case there is some compiler optimization going
on because I note that if all I do is add a loop that reads out bytes one by
one from the slice, it drops to 1000MB/s.

One outcome of this is that there's no sense going to sha256 - we should just
jump straight to sha512.