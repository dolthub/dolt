Concise Binary Object Representation (CBOR) is a superset of JSON's schema that's faster and more compact.

* http://tools.ietf.org/html/rfc7049
* http://cbor.io/

This repository contains implementations for Python and Go.

## Python ##
This Python implementation provides loads()/dumps() like the json standard library.

Compare to Python 2.7.5's standard library implementation of json:

```
#!

serialized 50000 objects into 1163097 cbor bytes in 0.05 seconds (1036613.48/s) and 1767001 json bytes in 0.22 seconds (224772.48/s)
compress to 999179 bytes cbor.gz and 1124500 bytes json.gz
load 50000 objects from cbor in 0.07 secs (763708.80/sec) and json in 0.32 (155348.97/sec)
```

There is also a pure-python implementation which gets about 1/3 the speed of json's C augmented speed.

Tested in Python 2.7.5, 2,7.6, 3.3.3, and 3.4.0

Available on pypi:

pip install cbor

## Go ##
The Go implementation is newer. It attempts to do serialization to/from struct types using reflection, but doesn't do 100% of cases like that right. It _should_ do everything fine serializing `map[string]interface{}` and `[]interface{}` and other basic types. It passes the test of decoding 100% of CBOR common appendix test strings.