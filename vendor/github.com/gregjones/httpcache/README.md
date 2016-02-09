httpcache
=========

A Transport for Go's http.Client that will cache responses according to the HTTP RFC

Package httpcache provides a http.RoundTripper implementation that works as a mostly RFC-compliant cache for http responses.

It is only suitable for use as a 'private' cache (i.e. for a web-browser or an API-client and not for a shared proxy).

**Documentation:** http://godoc.org/github.com/gregjones/httpcache

**License:** MIT (see LICENSE.txt)

Cache backends
--------------

- The built-in 'memory' cache stores responses in an in-memory map.
- https://github.com/gregjones/httpcache/diskcache provides a filesystem-backed cache using the [diskv](https://github.com/peterbourgon/diskv) library.
- https://github.com/gregjones/httpcache/memcache provides memcache implementations, for both App Engine and 'normal' memcache servers
- https://github.com/sourcegraph/s3cache uses Amazon S3 for storage.
- https://github.com/gregjones/httpcache/leveldbcache provides a filesystem-backed cache using [leveldb](https://github.com/syndtr/goleveldb/leveldb)
