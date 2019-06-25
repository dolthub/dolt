package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

const gitDoltVersion = 0

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
		err = link(remote)
	case "fetch":
		ptrFname := os.Args[2]
		err = fetch(ptrFname)
	case "update":
		ptrFname := os.Args[2]
		revision := os.Args[3]
		err = update(ptrFname, revision)
	default:
		die("Unknown command " + cmd)
	}

	if err != nil {
		die(err)
	}
}

func link(remote string) error {
	dirname := lastSegment(remote)
	if _, err := exec.Command("dolt", "clone", remote).Output(); err != nil {
		return fmt.Errorf("error cloning remote repository at %s: %v", remote, err)
	}

	revision, err := currentRevision(dirname)
	if err != nil {
		return err
	}

	ptrContents := fmt.Sprintf("version %d\nremote %s\nrevision %s\n", gitDoltVersion, remote, revision)
	if err := writeConfig(dirname, ptrContents); err != nil {
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

func fetch(ptrFname string) error {
	config, err := loadConfig(ptrFname)
	if err != nil {
		return err
	}

	if _, err := exec.Command("dolt", "clone", config.Remote).Output(); err != nil {
		return fmt.Errorf("error cloning remote repository at %s: %v", config.Remote, err)
	}

	dirname := lastSegment(config.Remote)
	checkoutCmd := exec.Command("dolt", "checkout", "-b", "git-dolt-pinned", config.Revision)
	checkoutCmd.Dir = dirname
	if _, err := checkoutCmd.Output(); err != nil {
		return fmt.Errorf("error checking out revision %s in directory %s: %v", config.Revision, dirname, err)
	}

	fmt.Printf("Dolt repository cloned from remote %s to directory %s at revision %s\n", config.Remote, dirname, config.Revision)
	return nil
}

func update(ptrFname string, revision string) error {
	ptrFname = EnsureSuffix(ptrFname, ".git-dolt")
	config, err := loadConfig(ptrFname)
	if err != nil {
		return err
	}

	config.Revision = revision

	if err := writeConfig(ptrFname, config.String()); err != nil {
		return err
	}
	fmt.Printf("Updated pointer file %s to revision %s\n", ptrFname, revision)
	return nil
}

func loadConfig(ptrFname string) (GitDoltConfig, error) {
	ptrFname = EnsureSuffix(ptrFname, ".git-dolt")
	ptrData, err := ioutil.ReadFile(ptrFname)
	if err != nil {
		return GitDoltConfig{}, fmt.Errorf("can't find pointer file %s", ptrFname)
	}

	config, err := ParseConfig(string(ptrData))
	if err != nil {
		return GitDoltConfig{}, fmt.Errorf("error parsing config file: %v", err)
	}

	return config, nil
}

func writeConfig(ptrFname string, ptrContents string) error {
	ptrFname = EnsureSuffix(ptrFname, ".git-dolt")
	if err := ioutil.WriteFile(ptrFname, []byte(ptrContents), 0644); err != nil {
		return fmt.Errorf("error writing git-dolt pointer file at %s: %v", ptrFname, err)
	}

	return nil
}

// EnsureSuffix adds a suffix to a string if not already present
func EnsureSuffix(s string, suffix string) string {
	if !strings.HasSuffix(s, suffix) {
		return s + suffix
	}
	return s
}

// GitDoltConfig represents the configuration for a git-dolt integration
type GitDoltConfig struct {
	// Version is the version of the git-dolt protocol being used
	Version int
	// Remote is the url of the dolt remote
	Remote string
	// Revision is the revision of the remote that this git-dolt pointer links to
	Revision string
}

// ParseConfig parses a git-dolt config string into a struct
func ParseConfig(c string) (GitDoltConfig, error) {
	lines := strings.Split(c, "\n")
	config := make(map[string]string)

	for _, line := range lines {
		setting := strings.Split(line, " ")
		if len(setting) == 2 {
			config[setting[0]] = setting[1]
		}
	}

	version, err := strconv.Atoi(config["version"])

	// default to the current version of git-dolt
	if err != nil {
		version = gitDoltVersion
	}

	requiredProps := []string{"remote", "revision"}

	for _, prop := range requiredProps {
		if _, ok := config[prop]; !ok {
			return GitDoltConfig{}, fmt.Errorf("no %s specified", prop)
		}
	}

	return GitDoltConfig{
		Version:  version,
		Remote:   config["remote"],
		Revision: config["revision"],
	}, nil
}

func (c GitDoltConfig) String() string {
	return fmt.Sprintf("version %d\nremote %s\nrevision %s\n", c.Version, c.Remote, c.Revision)
}

var hashRegex = regexp.MustCompile(`[0-9a-v]{32}`)

func currentRevision(dirname string) (string, error) {
	cmd := exec.Command("dolt", "log", "-n", "1")
	cmd.Dir = dirname
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error running dolt log to find current revision: %v", err)
	}
	return hashRegex.FindString(string(out)), nil
}

func lastSegment(s string) string {
	tokens := strings.Split(s, "/")
	return tokens[len(tokens)-1]
}

func die(reason interface{}) {
	fmt.Printf("Fatal: %v\n", reason)
	os.Exit(1)
}
