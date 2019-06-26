package commands

import (
	"fmt"
	"os/exec"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/config"
	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Fetch takes the filename of a git-dolt pointer file and clones
// the specified dolt repository to the specified revision.
func Fetch(ptrFname string) error {
	config, err := config.Load(ptrFname)
	if err != nil {
		return err
	}

	if _, err := exec.Command("dolt", "clone", config.Remote).Output(); err != nil {
		return fmt.Errorf("error cloning remote repository at %s: %v", config.Remote, err)
	}

	dirname := utils.LastSegment(config.Remote)
	checkoutCmd := exec.Command("dolt", "checkout", "-b", "git-dolt-pinned", config.Revision)
	checkoutCmd.Dir = dirname
	if _, err := checkoutCmd.Output(); err != nil {
		return fmt.Errorf("error checking out revision %s in directory %s: %v", config.Revision, dirname, err)
	}

	fmt.Printf("Dolt repository cloned from remote %s to directory %s at revision %s\n", config.Remote, dirname, config.Revision)
	return nil
}
