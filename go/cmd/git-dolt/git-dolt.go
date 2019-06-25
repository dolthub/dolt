package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/commands"
)

func main() {
	if _, err := exec.LookPath("dolt"); err != nil {
		die("It looks like Dolt is not installed on your system. Make sure that the `dolt` binary is in your PATH before attempting to run git-dolt commands.")
	}

	if len(os.Args) == 1 {
		fmt.Println("Dolt: It's Git for Data.")
		fmt.Println("Usage")
		return
	}

	var err error

	switch cmd := os.Args[1]; cmd {
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
		die("Unknown command " + cmd)
	}

	if err != nil {
		die(err)
	}
}

func die(reason interface{}) {
	fmt.Printf("Fatal: %v\n", reason)
	os.Exit(1)
}
