This is a js performance test rig which measures noms encoding/decoding along various axes.

As of July 7, these are the numbers I get on my Macbook Pro 2.7 i5.

Testing List: 		build 10000 			    scan 10000			    insert 200
numbers (8 B)		  307 ms (0.26 MB/s)		97 ms (0.82 MB/s)		344 ms (0.00 MB/s)
strings (32 B)		248 ms (1.29 MB/s)		72 ms (4.44 MB/s)		345 ms (0.02 MB/s)
structs (64 B)		493 ms (1.30 MB/s)		191 ms (3.35 MB/s)	237 ms (0.05 MB/s)

Testing Set: 		  build 10000 			    scan 10000			    insert 200
numbers (8 B)		  243 ms (0.33 MB/s)		77 ms (1.04 MB/s)		195 ms (0.01 MB/s)
strings (32 B)		264 ms (1.21 MB/s)		75 ms (4.27 MB/s)		262 ms (0.02 MB/s)
structs (64 B)		634 ms (1.01 MB/s)		199 ms (3.22 MB/s)	279 ms (0.05 MB/s)

Testing Map: 		  build 10000 			    scan 10000			    insert 200
numbers (8 B)		  255 ms (0.31 MB/s)		76 ms (1.05 MB/s)		263 ms (0.01 MB/s)
strings (32 B)		302 ms (1.06 MB/s)		74 ms (4.32 MB/s)		323 ms (0.02 MB/s)
structs (64 B)		757 ms (0.85 MB/s)		300 ms (2.13 MB/s)	282 ms (0.05 MB/s)

Testing Blob: 		build 2.10 MB			    scan 2.10 MB
			            588 ms (3.57 MB/s)		477 ms (4.40 MB/s)

