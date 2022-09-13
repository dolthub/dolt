package main

import (
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {

	p := cli.NewEphemeralPrinter()
	for i := 0; i < 10000; i++ {
		p.Printf("=========================")
		p.Display()
		time.Sleep(100 * time.Millisecond)
		p.Printf("+++++++++++++++++++")
		p.Display()
		time.Sleep(100 * time.Millisecond)
	}
}
