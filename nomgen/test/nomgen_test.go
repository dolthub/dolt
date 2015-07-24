package main

import (
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"os/exec"
)

// TestCodegen uses runs "go run gen/types.go" and then runs
// "go build" and finally "./test" which depends on types.go
func TestCodegen(t *testing.T) {
	assert := assert.New(t)

	_, thisfile, _, _ := runtime.Caller(0)
	dir := path.Dir(thisfile)
	os.Chdir(dir)

	cmd := exec.Command("go", "run", path.Join("gen", "types.go"))
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	assert.NoError(err, "go generate failed")

	cmd = exec.Command("go", "build")
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	assert.NoError(err)

	cmd = exec.Command("./test")
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	assert.NoError(err)

}
