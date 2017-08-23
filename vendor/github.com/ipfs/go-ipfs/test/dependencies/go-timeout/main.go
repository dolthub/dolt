package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <timeout-in-sec> <command ...>\n", os.Args[0])
		os.Exit(1)
	}
	timeout, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)

	cmd := exec.CommandContext(ctx, os.Args[2], os.Args[3:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
	err = cmd.Wait()

	if err != nil {
		if ctx.Err() != nil {
			os.Exit(124)
		} else {
			exitErr, ok := err.(*exec.ExitError)
			if !ok {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(255)
			}
			waits, ok := exitErr.Sys().(syscall.WaitStatus)
			if !ok {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(255)
			}
			os.Exit(waits.ExitStatus())
		}
	} else {
		os.Exit(0)
	}
}
