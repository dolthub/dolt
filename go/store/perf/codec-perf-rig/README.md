This is a Go performance test rig which measures noms encoding/decoding along various axes.

As of July 7, these are the numbers I get on my Macbook Pro 2.7 i5.

Testing List: 		build 100000			    scan 100000			      insert 2000
numbers (8 B)     451 ms (1.77 MB/s)		26 ms (30.26 MB/s)		275 ms (0.06 MB/s)
strings (32 B)		444 ms (7.20 MB/s)		35 ms (90.04 MB/s)		321 ms (0.20 MB/s)
structs (64 B)		682 ms (9.38 MB/s)		115 ms (55.34 MB/s)		239 ms (0.53 MB/s)

Testing Set: 		  build 100000			    scan 100000			    insert 2000
numbers (8 B)		  461 ms (1.73 MB/s)		23 ms (33.62 MB/s)		90 ms (0.18 MB/s)
strings (32 B)		480 ms (6.67 MB/s)		28 ms (112.76 MB/s)		207 ms (0.31 MB/s)
structs (64 B)		1608 ms (3.98 MB/s)		113 ms (56.29 MB/s)		306 ms (0.42 MB/s)

Testing Map: 		  build 100000			    scan 100000			    insert 2000
numbers (8 B)		  564 ms (1.42 MB/s)		44 ms (18.01 MB/s)		121 ms (0.13 MB/s)
strings (32 B)		586 ms (5.46 MB/s)		61 ms (51.74 MB/s)		521 ms (0.12 MB/s)
structs (64 B)		2005 ms (3.19 MB/s)		260 ms (24.61 MB/s)		530 ms (0.24 MB/s)

Testing Blob: 		build 33 MB			      scan 33 MB
                  553 ms (60.60 MB/s)		429 ms (78.21 MB/s)


