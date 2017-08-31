package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"syscall"
	"time"

	manet "gx/ipfs/QmX3U3YXCQ6UYBxq2LVWF8dARS1hPUTEYLrSx654Qyxyw6/go-multiaddr-net"
	ma "gx/ipfs/QmXY77cVe7rVRQXZZQRioukUM7aRW3BTcAgJe12MCtb3Ji/go-multiaddr"
)

var usageText = `usage: %s <fd-num> <multiaddr>

open <fd-num> file descriptors at <multiaddr>, and keep them open
until the process is killed. This command useful for test suites.
For example:

    # open 16 tcp sockets at 127.0.0.1:80
    %s 16 /ip4/127.0.0.1/tcp/80
`

// TODO:
// # open 64 utp sockets at 127.0.0.1:8080
// %s 64 /ip4/127.0.0.1/udp/8080/utp

// # open 1024 unix domain sockets at /foo/var.sock
// %s 1024 /uds/foo/var.sock

func fdRaise(nn int) error {
	n := uint64(nn)

	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}

	if rLimit.Cur >= n {
		fmt.Printf("already at %d >= %d fds\n", rLimit.Cur, n)
		return nil // all good.
	}
	rLimit.Cur = n

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}

	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}

	if rLimit.Cur < n {
		return fmt.Errorf("failed to raise fd limit to %d (still %d)", n, rLimit.Cur)
	}

	fmt.Printf("raised fds to %d >= %d fds\n", rLimit.Cur, n)
	return nil
}

func readUntilErr(r io.Reader) error {
	buf := make([]byte, 1024)
	for {
		_, err := r.Read(buf)
		if err != nil {
			return err
		}
	}
}

func dialAndHang(i int, m ma.Multiaddr, errs chan<- error) {
	c, err := manet.Dial(m)
	if err != nil {
		errs <- err
		return
	}

	fmt.Printf("conn %d connected\n", i)
	// read until proc exits or conn closes.
	go func() {
		errs <- readUntilErr(c)
	}()
}

func fdHang(n int, m ma.Multiaddr, hold time.Duration) error {
	// first, make sure we raise our own fds to be enough.
	if err := fdRaise(n + 10); err != nil {
		return err
	}

	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		// this sleep is here because OSX fails to be able to dial and listen
		// as fast as Go tries to issue the commands. this seems to be a crap
		// os failure.
		time.Sleep(time.Millisecond)

		dialAndHang(i, m, errs)
	}

	var deadline <-chan time.Time
	if hold != 0 {
		deadline = time.After(hold)
	}

	var lastErr error
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if err != nil && err != io.EOF {
				fmt.Printf("conn %d error: %s\n", i, err)
				lastErr = err
			}
		case <-deadline:
			fmt.Println("times up! exiting...")
			return lastErr
		}
	}

	fmt.Println("done")
	return lastErr
}

func fatal(i interface{}) {
	fmt.Println(i)
	os.Exit(1)
}

func main() {
	flag.Usage = func() {
		p := os.Args[0]
		fmt.Printf(usageText, p, p)
	}

	hold := flag.String("hold", "0", "time to hold connections open for (0 implies forever)")
	flag.Parse()

	if len(flag.Args()) != 2 {
		flag.Usage()
		os.Exit(1)
		return
	}

	n, err := strconv.Atoi(flag.Args()[0])
	if err != nil {
		fatal("<fd-num> argument must be a number")
	}

	m, err := ma.NewMultiaddr(flag.Args()[1])
	if err != nil {
		fatal("<multiaddr> argument must be a valid multiaddr")
	}

	var holddur time.Duration
	if *hold != "0" {
		dur, err := time.ParseDuration(*hold)
		if err != nil {
			fatal(err)
		}
		holddur = dur
	}

	fmt.Printf("hanging %d fds at %s\n", n, m)
	if err := fdHang(n, m, holddur); err != nil {
		fatal(err)
	}
}
