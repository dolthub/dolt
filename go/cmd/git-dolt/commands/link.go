package commands

import (
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/doltops"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Link creates a git-dolt pointer file linking the given dolt remote
// to the current git repository.
func Link(remote string) error {
	if err := doltops.Clone(remote); err != nil {
		return err
	}
	
	dirname := utils.LastSegment(remote)
	revision, err := utils.CurrentRevision(dirname)
	if err != nil {
		return err
	}

	c := config.GitDoltConfig{Version: env.Version, Remote: remote, Revision: revision}
	if err := config.Write(dirname, c.String()); err != nil {
		return err
	}

	if err := utils.AppendToFile(".gitignore", dirname); err != nil {
		return err
	}

	fmt.Printf("\nDolt repository linked!\n\n")
	fmt.Printf("* Repository cloned to %s at revision %s\n", dirname, revision)
	fmt.Printf("* Pointer file created at %s.git-dolt\n", dirname)
	fmt.Printf("* %s added to .gitignore\n\n", dirname)
	fmt.Println("You should git commit these results.")
	return nil
}
