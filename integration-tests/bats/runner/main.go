package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

var NumRunners = flag.Int("n", 8, "num runners")
var Dir = flag.String("d", "..", "directory to find .bats files in")

func main() {
	flag.Parse()

	names, err := GetTestFileNames()
	if err != nil {
		panic(err)
	}

	testnames := make(chan string)

	// protects failedfiles until errgroup is done.
	mu := new(sync.Mutex)
	failedfiles := []string{}

	eg, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < *NumRunners; i++ {
		eg.Go(func() error {
			for {
				select {
				case testname, ok := <-testnames:
					if !ok {
						return nil
					}
					err := RunTestFile(ctx, testname)
					if err != nil {
						mu.Lock()
						failedfiles = append(failedfiles, testname)
						mu.Unlock()
					}
				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	eg.Go(func() error {
		defer close(testnames)
		for _, name := range names {
			select {
			case testnames <- name:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})

	err = eg.Wait()
	if err != nil {
		panic(fmt.Sprintf("unexpected error running tests: %v", err))
	}

	if len(failedfiles) > 0 {
		fmt.Println("failed the following test files.")
		for _, f := range failedfiles {
			fmt.Println(f)
		}
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

func GetTestFileNames() ([]string, error) {
	if false {
		return []string{
			"../remotes.bats",
		}, nil
	}
	err := os.Chdir(*Dir)
	if err != nil {
		return nil, err
	}
	dir, err := os.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	fis, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for _, fi := range fis {
		name := fi.Name()
		if strings.HasSuffix(name, ".bats") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func RunTestFile(ctx context.Context, name string) error {
	c := exec.CommandContext(ctx, "bats", "--tap", name)
	o, err := c.CombinedOutput()
	fmt.Println(name)
	fmt.Println(string(o))
	return err
}
