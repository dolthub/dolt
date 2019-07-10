package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/commands"
)

func main() {
	if _, err := exec.LookPath("dolt"); err != nil {
		log.Fatal("It looks like Dolt is not installed on your system. Make sure that the `dolt` binary is in your PATH before attempting to run git-dolt commands.")
	}

	if len(os.Args) == 1 {
		fmt.Println("Dolt: It's Git for Data.")
		printUsage()
		return
	}

	var err error

	switch cmd := os.Args[1]; cmd {
	case "install":
		err = commands.Install()
	case "link":
		remote := os.Args[2]
		err = commands.Link(remote)
	case "fetch":
		ptrFname := os.Args[2]
		err = commands.Fetch(ptrFname)
	case "update":
		ptrFname := os.Args[2]
		revision := os.Args[3]
		err = commands.Update(ptrFname, revision)
	default:
		printUsage()
		log.Fatalf("Unknown command %s\n", cmd)
	}

	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}
}

func printUsage() {
	fmt.Println("Usage")
}
