/*
Package badger implements an embeddable, simple and fast key-value store, written in pure Go.
It is designed to be highly performant for both reads and writes simultaneously.
Badger uses LSM tree, along with a value log, to separate keys from values, hence reducing
both write amplification and the size of the LSM tree.
This allows LSM tree to be served entirely from RAM, while the values are served from SSD.
As values get larger, this results in increasingly faster Set() and Get() performance than top
of the line KV stores in market today.
*/
package badger
