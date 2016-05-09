This is a performance test rig for the two main types of hashing we do in NOMS.

This also has a mode 'rb', what reads bytes one out of a time from the Node
buffer, but doesn't hash them. This is useful to understand how much
'headrooom' there is just making the JS impl of buzhash faster.

As of May 9, 2016, these are the numbers I get on a macbook pro 3.1 GHz Intel
Core i7:

no hashing : 516 MB/s
sha1 only  : 289 MB/s
rb only    :  83 MB/s
bh only    :  28 MB/s
sha1 and rb:  73 MB/s
sha1 and bh:  28 MB/s

My interpretation is that there is about 43 (71-28) MB/s on the table without
doing anything crazy. Because that's the diff btwn sha1+rb and sha1+bh. So
we're spending 43 MB/s doing buzhash in JS.