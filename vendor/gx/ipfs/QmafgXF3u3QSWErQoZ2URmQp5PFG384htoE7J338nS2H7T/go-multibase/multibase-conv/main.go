package main

import (
	"fmt"
	"os"

	multibase "gx/ipfs/QmafgXF3u3QSWErQoZ2URmQp5PFG384htoE7J338nS2H7T/go-multibase"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: %s <new-base> <multibase-str>...\n", os.Args[0])
		os.Exit(1)
	}

	var newBase multibase.Encoding
	if baseParm := os.Args[1]; len(baseParm) != 0 {
		newBase = multibase.Encoding(baseParm[0])
	} else {
		fmt.Fprintln(os.Stderr, "<new-base> is empty")
		os.Exit(1)
	}

	input := os.Args[2:]

	for _, strmbase := range input {
		_, data, err := multibase.Decode(strmbase)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error while decoding: %s\n", err)
			os.Exit(1)
		}

		newCid, err := multibase.Encode(newBase, data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error while encoding: %s\n", err)
			os.Exit(1)
		}
		fmt.Println(newCid)
	}

}
