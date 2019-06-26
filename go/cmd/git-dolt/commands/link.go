package commands

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Link creates a git-dolt pointer file linking the given dolt remote
// to the current git repository.
func Link(remote string) error {
	dirname := utils.LastSegment(remote)
	if _, err := exec.Command("dolt", "clone", remote).Output(); err != nil {
		return fmt.Errorf("error cloning remote repository at %s: %v", remote, err)
	}

	revision, err := utils.CurrentRevision(dirname)
	if err != nil {
		return err
	}

	c := config.GitDoltConfig{Version: env.Version, Remote: remote, Revision: revision}
	if err := config.Write(dirname, c.String()); err != nil {
		return err
	}

	giFile, err := os.OpenFile(".gitignore", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening .gitignore: %v", err)
	}
	defer giFile.Close()

	if _, err = giFile.WriteString(fmt.Sprintf("%s\n", dirname)); err != nil {
		return fmt.Errorf("error writing to .gitignore at %v", err)
	}

	fmt.Printf("\nDolt repository successfully linked!\n\n")
	fmt.Printf("* Dolt repository cloned to %s at revision %s\n", dirname, revision)
	fmt.Printf("* Pointer file created at %s.git-dolt\n", dirname)
	fmt.Printf("* %s added to .gitignore\n\n", dirname)
	fmt.Println("You should git commit these results.")
	return nil
}
