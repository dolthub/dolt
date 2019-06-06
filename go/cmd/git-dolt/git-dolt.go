package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

const gitDoltVersion = 0

func main() {
	nArgs := len(os.Args)

	if nArgs == 1 {
		fmt.Println("Dolt: It's Git for Data.")
		fmt.Println("Usage")
		return
	}

	cmd := os.Args[1]

	if cmd == "link" {
		remote := os.Args[2]
		dirname := lastSegment(remote)
		_, err := exec.Command("dolt", "clone", remote, "--insecure").Output()
		check(err)

		revision := currentRevision(dirname)

		ptrFile, err := os.Create(fmt.Sprintf("%s.git-dolt", dirname))
		check(err)
		defer ptrFile.Close()

		_, err = ptrFile.WriteString(fmt.Sprintf("version %d\nremote %s\nrevision %s\n", gitDoltVersion, remote, revision))
		check(err)

		giFile, err := os.OpenFile(".gitignore", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		check(err)
		defer giFile.Close()

		_, err = giFile.WriteString(fmt.Sprintf("%s\n", dirname))
		check(err)

		fmt.Printf("\nSuccess!\n\n")
		fmt.Printf("* Dolt repository cloned to %s at revision %s\n", dirname, revision)
		fmt.Printf("* Pointer file created at %s.git-dolt\n", dirname)
		fmt.Printf("* %s added to .gitignore\n\nYou should git commit these results.\n", dirname)
	}
}

var hashRegex = regexp.MustCompile(`[0-9a-v]{32}`)

func currentRevision(dirname string) string {
	cmd := exec.Command("dolt", "log", "-n", "1")
	cmd.Dir = dirname
	out, err := cmd.Output()
	check(err)
	return hashRegex.FindString(string(out))
}

func lastSegment(s string) string {
	return strings.Split(s, "/")[2]
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
