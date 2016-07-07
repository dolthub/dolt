This is a js performance test rig which measures noms encoding/decoding along various axes.

As of July 7, these are the numbers I get on my Macbook Pro 2.7 i5.

Testing List: 		build 10000 			    scan 10000			      insert 200
numbers (8 B)		  274 ms (0.29 MB/s)		94 ms (0.85 MB/s)		  359 ms (0.00 MB/s)
strings (32 B)		223 ms (1.43 MB/s)		100 ms (3.20 MB/s)		337 ms (0.02 MB/s)
structs (64 B)		591 ms (1.08 MB/s)		354 ms (1.81 MB/s)		342 ms (0.04 MB/s)

Testing Set: 		  build 10000 			    scan 10000			      insert 200
numbers (8 B)		  185 ms (0.43 MB/s)		91 ms (0.88 MB/s)		  320 ms (0.01 MB/s)
strings (32 B)		226 ms (1.42 MB/s)		65 ms (4.92 MB/s)		  259 ms (0.02 MB/s)
structs (64 B)		764 ms (0.84 MB/s)		349 ms (1.83 MB/s)		252 ms (0.05 MB/s)

Testing Map: 		  build 10000 			    scan 10000			      insert 200
numbers (8 B)		  235 ms (0.34 MB/s)		63 ms (1.27 MB/s)		  271 ms (0.01 MB/s)
strings (32 B)		575 ms (0.56 MB/s)		103 ms (3.11 MB/s)		427 ms (0.01 MB/s)
structs (64 B)		1025 ms (0.62 MB/s)		611 ms (1.05 MB/s)		423 ms (0.03 MB/s)

Testing Blob: 		build 2.10 MB			    scan 2.10 MB
                  580 ms (3.62 MB/s)		470 ms (4.46 MB/s)


