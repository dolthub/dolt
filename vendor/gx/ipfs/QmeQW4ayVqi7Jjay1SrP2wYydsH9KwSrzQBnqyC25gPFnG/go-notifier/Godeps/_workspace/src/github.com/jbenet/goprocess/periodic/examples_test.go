package periodicproc_test

import (
	"fmt"
	"time"

	goprocess "gx/ipfs/QmeQW4ayVqi7Jjay1SrP2wYydsH9KwSrzQBnqyC25gPFnG/go-notifier/Godeps/_workspace/src/github.com/jbenet/goprocess"
	periodicproc "gx/ipfs/QmeQW4ayVqi7Jjay1SrP2wYydsH9KwSrzQBnqyC25gPFnG/go-notifier/Godeps/_workspace/src/github.com/jbenet/goprocess/periodic"
)

func ExampleEvery() {
	tock := make(chan struct{})

	i := 0
	p := periodicproc.Every(time.Second, func(proc goprocess.Process) {
		tock <- struct{}{}
		fmt.Printf("hello %d\n", i)
		i++
	})

	<-tock
	<-tock
	<-tock
	p.Close()

	// Output:
	// hello 0
	// hello 1
	// hello 2
}

func ExampleTick() {
	p := periodicproc.Tick(time.Second, func(proc goprocess.Process) {
		fmt.Println("tick")
	})

	<-time.After(3*time.Second + 500*time.Millisecond)
	p.Close()

	// Output:
	// tick
	// tick
	// tick
}

func ExampleTickGo() {

	// with TickGo, execution is not rate limited,
	// there can be many in-flight simultaneously

	wait := make(chan struct{})
	p := periodicproc.TickGo(time.Second, func(proc goprocess.Process) {
		fmt.Println("tick")
		<-wait
	})

	<-time.After(3*time.Second + 500*time.Millisecond)

	wait <- struct{}{}
	wait <- struct{}{}
	wait <- struct{}{}
	p.Close() // blocks us until all children are closed.

	// Output:
	// tick
	// tick
	// tick
}

func ExampleOnSignal() {
	sig := make(chan struct{})
	p := periodicproc.OnSignal(sig, func(proc goprocess.Process) {
		fmt.Println("fire!")
	})

	sig <- struct{}{}
	sig <- struct{}{}
	sig <- struct{}{}
	p.Close()

	// Output:
	// fire!
	// fire!
	// fire!
}
