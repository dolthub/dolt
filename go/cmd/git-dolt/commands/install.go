package commands

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/cmd/git-dolt/utils"
)

// Install configures this git repository for use with git-dolt; specifically, it sets up the
// smudge filter that automatically clones dolt repos when git-dolt pointer files are checked out.
func Install() error {
	if _, err := exec.LookPath("git-dolt-smudge"); err != nil {
		return fmt.Errorf("can't find git-dolt-smudge in PATH")
	}

	gitPath, err := utils.FindGitConfigUnderRoot()
	if err != nil {
		return err
	}

	gitParentPath := filepath.Dir(gitPath)
	gitAttributesPath := filepath.Join(gitParentPath, ".gitattributes")
	if err := utils.AppendToFile(gitAttributesPath, "*.git-dolt filter=git-dolt"); err != nil {
		return err
	}

	gitConfigPath := filepath.Join(gitPath, "config")
	if err := utils.AppendToFile(gitConfigPath, "[filter \"git-dolt\"]\n\tsmudge = git-dolt-smudge"); err != nil {
		return err
	}

	fmt.Println("Installed git-dolt smudge filter. When git-dolt pointer files are checked out in this git repository, the corresponding Dolt repositories will be automatically cloned.")
	fmt.Println("\nYou should git commit the changes to .gitattributes.")
	return nil
}
