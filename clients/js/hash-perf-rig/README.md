This is a performance test rig for the two main types of hashing we do in NOMS. There is also a 'rb' mode, which reads bytes out of a the Node buffer, but doesn't do anything to them. The reason this is slower than sha1 alone is because the sha1 is implemented in native code.

As of May 11, 2016, these are the numbers I get on a macbook pro 3.1 GHz Intel Core i7, using Node 5.3:

```
no hashing : 1100 MB/s
sha1 only  :  459 MB/s
rb only    :  362 MB/s
bh only    :  150 MB/s
sha1 and rb:  246 MB/s
sha1 and bh:  125 MB/s
```

This makes it seem like there ought to still be significant headroom to make buzhash faster.
