package editor

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

//OpenCommitEditor allows user to write/edit commit message in temporary file
func OpenCommitEditor(ed string, initialContents string) (string, error) {
	filename := filepath.Join(os.TempDir(), uuid.New().String())
	err := ioutil.WriteFile(filename, []byte(initialContents), os.ModePerm)

	if err != nil {
		return "", err
	}

	cmdName, cmdArgs := getEditorString(ed)

	if cmdName == "" {
		cmdName = os.Getenv("EDITOR")
		cmdArgs = []string{}
	}

	cmdArgs = append(cmdArgs, filename)

	cmd := exec.Command(cmdName, cmdArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		fmt.Printf("Start failed: %s", err)
	}
	fmt.Printf("Waiting for command to finish.\n")
	err = cmd.Wait()
	fmt.Printf("Command finished with error: %v\n", err)

	data, err := ioutil.ReadFile(filename)

	if err != nil {
		return "", err
	}

	return string(data), nil
}

func getEditorString(edStr string) (string, []string) {
	splitStr := strings.Split(edStr, " ")
	return splitStr[0], splitStr[1:]
}
