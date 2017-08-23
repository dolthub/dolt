thirdparty consists of Golang packages that contain no go-ipfs dependencies and
may be vendored ipfs/go-ipfs at a later date.

packages in under this directory _must not_ import packages under
`ipfs/go-ipfs` that are not also under `thirdparty`.
