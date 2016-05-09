This is a performance test rig for the two main types of hashing we do in NOMS.

As of May 9, these are the numbers I get on a macbook pro 3.1 GHz Intel Core i7.

- no hashing : 3000 MB/s
- sha1 only  :  450 MB/s
- bh only    :  134 MB/s
- sha1 and bh:  107 MB/s

I think that in the no hashing case there is some compiler optimization going
on because I note that if all I do is add a loop that reads out bytes one by
one from the slice, it drops to 1000MB/s.
