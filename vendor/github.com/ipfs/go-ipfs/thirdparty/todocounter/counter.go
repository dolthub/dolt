package todocounter

import (
	"sync"
)

// Counter records things remaining to process. It is needed for complicated
// cases where multiple goroutines are spawned to process items, and they may
// generate more items to process. For example, say a query over a set of nodes
// may yield either a result value, or more nodes to query. Signaling is subtly
// complicated, because the queue may be empty while items are being processed,
// that will end up adding more items to the queue.
//
// Use Counter like this:
//
//    todos := make(chan int, 10)
//    ctr := todoctr.NewCounter()
//
//    process := func(item int) {
//      fmt.Println("processing %d\n...", item)
//
//      // this task may randomly generate more tasks
//      if rand.Intn(5) == 0 {
//        todos<- item + 1
//        ctr.Increment(1) // increment counter for new task.
//      }
//
//      ctr.Decrement(1) // decrement one to signal the task being done.
//    }
//
//    // add some tasks.
//    todos<- 1
//    todos<- 2
//    todos<- 3
//    todos<- 4
//    ctr.Increment(4)
//
//    for {
//      select {
//      case item := <- todos:
//        go process(item)
//      case <-ctr.Done():
//        fmt.Println("done processing everything.")
//        close(todos)
//      }
//    }
type Counter interface {
	// Incrememnt adds a number of todos to track.
	// If the counter is **below** zero, it panics.
	Increment(i uint32)

	// Decrement removes a number of todos to track.
	// If the count drops to zero, signals done and destroys the counter.
	// If the count drops **below** zero, panics. It means you have tried to remove
	// more things than you added, i.e. sync issues.
	Decrement(i uint32)

	// Done returns a channel to wait upon. Use it in selects:
	//
	//  select {
	//  case <-ctr.Done():
	//    // done processing all items
	//  }
	//
	Done() <-chan struct{}
}

type todoCounter struct {
	count int32
	done  chan struct{}
	sync.RWMutex
}

// NewSyncCounter constructs a new counter
func NewSyncCounter() Counter {
	return &todoCounter{
		done: make(chan struct{}),
	}
}

func (c *todoCounter) Increment(i uint32) {
	c.Lock()
	defer c.Unlock()

	if c.count < 0 {
		panic("counter already signaled done. use a new counter.")
	}

	// increment count
	c.count += int32(i)
}

// Decrement removes a number of todos to track.
// If the count drops to zero, signals done and destroys the counter.
// If the count drops **below** zero, panics. It means you have tried to remove
// more things than you added, i.e. sync issues.
func (c *todoCounter) Decrement(i uint32) {
	c.Lock()
	defer c.Unlock()

	if c.count < 0 {
		panic("counter already signaled done. probably have sync issues.")
	}

	if int32(i) > c.count {
		panic("decrement amount creater than counter. sync issues.")
	}

	c.count -= int32(i)
	if c.count == 0 { // done! signal it.
		c.count--     // set it to -1 to prevent reuse
		close(c.done) // a closed channel will always return nil
	}
}

func (c *todoCounter) Done() <-chan struct{} {
	return c.done
}
