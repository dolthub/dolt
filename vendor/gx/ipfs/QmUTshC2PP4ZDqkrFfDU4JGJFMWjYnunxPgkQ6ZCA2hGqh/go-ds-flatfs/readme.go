package flatfs

var README_IPFS_DEF_SHARD =
	`This is a repository of IPLD objects. Each IPLD object is in a single file,
named <base32 encoding of cid>.data. Where <base32 encoding of cid> is the
"base32" encoding of the CID (as specified in
https://github.com/multiformats/multibase) without the 'B' prefix.
All the object files are placed in a tree of directories, based on a
function of the CID. This is a form of sharding similar to
the objects directory in git repositories. Previously, we used
prefixes, we now use the next-to-last two charters.

    func NextToLast(base32cid string) {
      nextToLastLen := 2
      offset := len(base32cid) - nextToLastLen - 1
      return str[offset : offset+nextToLastLen]
    }

For example, an object with a base58 CIDv1 of

    zb2rhYSxw4ZjuzgCnWSt19Q94ERaeFhu9uSqRgjSdx9bsgM6f

has a base32 CIDv1 of

    BAFKREIA22FLID5AJ2KU7URG47MDLROZIH6YF2KALU2PWEFPVI37YLKRSCA

and will be placed at

    SC/AFKREIA22FLID5AJ2KU7URG47MDLROZIH6YF2KALU2PWEFPVI37YLKRSCA.data

with 'SC' being the last-to-next two characters and the 'B' at the
beginning of the CIDv1 string is the multibase prefix that is not
stored in the filename.
`
