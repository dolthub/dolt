package mfsr

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

const VersionFile = "version"

type RepoPath string

func (rp RepoPath) VersionFile() string {
	return path.Join(string(rp), VersionFile)
}

func (rp RepoPath) Version() (int, error) {
	if rp == "" {
		return 0, fmt.Errorf("invalid repo path \"%s\"", rp)
	}

	fn := rp.VersionFile()
	if _, err := os.Stat(fn); err != nil {
		return 0, err
	}

	c, err := ioutil.ReadFile(fn)
	if err != nil {
		return 0, err
	}

	s := strings.TrimSpace(string(c))
	return strconv.Atoi(s)
}

func (rp RepoPath) CheckVersion(version int) error {
	v, err := rp.Version()
	if err != nil {
		return err
	}

	if v != version {
		return fmt.Errorf("versions differ (expected: %d, actual:%d)", version, v)
	}

	return nil
}

func (rp RepoPath) WriteVersion(version int) error {
	fn := rp.VersionFile()
	return ioutil.WriteFile(fn, []byte(fmt.Sprintf("%d\n", version)), 0644)
}
