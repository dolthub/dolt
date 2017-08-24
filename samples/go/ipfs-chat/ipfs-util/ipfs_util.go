// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
    "flag"
    
    cid "gx/ipfs/QmTprEaAA2A9bst5XH7exuyi5KzNMK3SEDNN8rBDnKWcUS/go-cid"
    mh "gx/ipfs/QmU9a9NV9RdPNwZQDYd5uKsm6N6LJLSvLbywDDYFbaaC6P/go-multihash"
    "github.com/attic-labs/noms/go/d"
    "github.com/attic-labs/noms/go/hash"
    "fmt"
    "os"
)

func main() {
    nomsHash := flag.String("nh", "", "noms hash to translate to ipfs hash")
    ipfsHash := flag.String("ih", "", "ipfs hash to translate to noms hash")
    
    flag.Parse()
    
    if *nomsHash != "" {
        nh, ok := hash.MaybeParse(*nomsHash)
        if !ok {
            fmt.Printf("unable to parse noms hash: %s\n", *nomsHash)
            os.Exit(1)
        }
        ih := nomsHashToCID(nh)
        fmt.Println("ipfs hash:", ih.String())
        return
    }
    
    ih, err := cid.Decode(*ipfsHash)
    if err != nil {
        fmt.Printf("unable to parse ipfs hash: %s, error: %s\n", *ipfsHash, err)
        os.Exit(1)
    }
    nh := cidToNomsHash(ih)
    fmt.Println("noms hash:", nh.String())
}

func nomsHashToCID(nh hash.Hash) *cid.Cid {
    mhb, err := mh.Encode(nh[:], mh.SHA2_512)
    d.PanicIfError(err)
    return cid.NewCidV1(cid.Raw, mhb)
}

func cidToNomsHash(id *cid.Cid) (h hash.Hash) {
    dmh, err := mh.Decode([]byte(id.Hash()))
    d.PanicIfError(err)
    copy(h[:], dmh.Digest)
    return
}
