package query

import (
	goprocess "gx/ipfs/QmSF8fPo3jgVBAy8fpdjjYqgG87dkJgUprRBHRd2tmfgpP/goprocess"
)

/*
Query represents storage for any key-value pair.

tl;dr:

  queries are supported across datastores.
  Cheap on top of relational dbs, and expensive otherwise.
  Pick the right tool for the job!

In addition to the key-value store get and set semantics, datastore
provides an interface to retrieve multiple records at a time through
the use of queries. The datastore Query model gleans a common set of
operations performed when querying. To avoid pasting here years of
database research, let’s summarize the operations datastore supports.

Query Operations:

  * namespace - scope the query, usually by object type
  * filters - select a subset of values by applying constraints
  * orders - sort the results by applying sort conditions
  * limit - impose a numeric limit on the number of results
  * offset - skip a number of results (for efficient pagination)

datastore combines these operations into a simple Query class that allows
applications to define their constraints in a simple, generic, way without
introducing datastore specific calls, languages, etc.

Of course, different datastores provide relational query support across a
wide spectrum, from full support in traditional databases to none at all in
most key-value stores. Datastore aims to provide a common, simple interface
for the sake of application evolution over time and keeping large code bases
free of tool-specific code. It would be ridiculous to claim to support high-
performance queries on architectures that obviously do not. Instead, datastore
provides the interface, ideally translating queries to their native form
(e.g. into SQL for MySQL).

However, on the wrong datastore, queries can potentially incur the high cost
of performing the aforemantioned query operations on the data set directly in
Go. It is the client’s responsibility to select the right tool for the job:
pick a data storage solution that fits the application’s needs now, and wrap
it with a datastore implementation. As the needs change, swap out datastore
implementations to support your new use cases. Some applications, particularly
in early development stages, can afford to incurr the cost of queries on non-
relational databases (e.g. using a FSDatastore and not worry about a database
at all). When it comes time to switch the tool for performance, updating the
application code can be as simple as swapping the datastore in one place, not
all over the application code base. This gain in engineering time, both at
initial development and during later iterations, can significantly offset the
cost of the layer of abstraction.

*/
type Query struct {
	Prefix   string   // namespaces the query to results whose keys have Prefix
	Filters  []Filter // filter results. apply sequentially
	Orders   []Order  // order results. apply sequentially
	Limit    int      // maximum number of results
	Offset   int      // skip given number of results
	KeysOnly bool     // return only keys.
}

// NotFetched is a special type that signals whether or not the value
// of an Entry has been fetched or not. This is needed because
// datastore implementations get to decide whether Query returns values
// or only keys. nil is not a good signal, as real values may be nil.
const NotFetched int = iota

// Entry is a query result entry.
type Entry struct {
	Key   string // cant be ds.Key because circular imports ...!!!
	Value interface{}
}

// Result is a special entry that includes an error, so that the client
// may be warned about internal errors.
type Result struct {
	Entry

	Error error
}

// Results is a set of Query results. This is the interface for clients.
// Example:
//
//   qr, _ := myds.Query(q)
//   for r := range qr.Next() {
//     if r.Error != nil {
//       // handle.
//       break
//     }
//
//     fmt.Println(r.Entry.Key, r.Entry.Value)
//   }
//
// or, wait on all results at once:
//
//   qr, _ := myds.Query(q)
//   es, _ := qr.Rest()
//   for _, e := range es {
//     	fmt.Println(e.Key, e.Value)
//   }
//
type Results interface {
	Query() Query             // the query these Results correspond to
	Next() <-chan Result      // returns a channel to wait for the next result
	NextSync() (Result, bool) // blocks and waits to return the next result, second paramter returns false when results are exhausted
	Rest() ([]Entry, error)   // waits till processing finishes, returns all entries at once.
	Close() error             // client may call Close to signal early exit

	// Process returns a goprocess.Process associated with these results.
	// most users will not need this function (Close is all they want),
	// but it's here in case you want to connect the results to other
	// goprocess-friendly things.
	Process() goprocess.Process
}

// results implements Results
type results struct {
	query Query
	proc  goprocess.Process
	res   <-chan Result
}

func (r *results) Next() <-chan Result {
	return r.res
}

func (r *results) NextSync() (Result, bool) {
	val, ok := <-r.res
	return val, ok
}

func (r *results) Rest() ([]Entry, error) {
	var es []Entry
	for e := range r.res {
		if e.Error != nil {
			return es, e.Error
		}
		es = append(es, e.Entry)
	}
	<-r.proc.Closed() // wait till the processing finishes.
	return es, nil
}

func (r *results) Process() goprocess.Process {
	return r.proc
}

func (r *results) Close() error {
	return r.proc.Close()
}

func (r *results) Query() Query {
	return r.query
}

// ResultBuilder is what implementors use to construct results
// Implementors of datastores and their clients must respect the
// Process of the Request:
//
//   * clients must call r.Process().Close() on an early exit, so
//     implementations can reclaim resources.
//   * if the Entries are read to completion (channel closed), Process
//     should be closed automatically.
//   * datastores must respect <-Process.Closing(), which intermediates
//     an early close signal from the client.
//
type ResultBuilder struct {
	Query   Query
	Process goprocess.Process
	Output  chan Result
}

// Results returns a Results to to this builder.
func (rb *ResultBuilder) Results() Results {
	return &results{
		query: rb.Query,
		proc:  rb.Process,
		res:   rb.Output,
	}
}

const NormalBufSize = 1
const KeysOnlyBufSize = 128

func NewResultBuilder(q Query) *ResultBuilder {
	bufSize := NormalBufSize
	if q.KeysOnly {
		bufSize = KeysOnlyBufSize
	}
	b := &ResultBuilder{
		Query:  q,
		Output: make(chan Result, bufSize),
	}
	b.Process = goprocess.WithTeardown(func() error {
		close(b.Output)
		return nil
	})
	return b
}

// ResultsWithChan returns a Results object from a channel
// of Result entries. Respects its own Close()
func ResultsWithChan(q Query, res <-chan Result) Results {
	b := NewResultBuilder(q)

	// go consume all the entries and add them to the results.
	b.Process.Go(func(worker goprocess.Process) {
		for {
			select {
			case <-worker.Closing(): // client told us to close early
				return
			case e, more := <-res:
				if !more {
					return
				}

				select {
				case b.Output <- e:
				case <-worker.Closing(): // client told us to close early
					return
				}
			}
		}
		return
	})

	go b.Process.CloseAfterChildren()
	return b.Results()
}

// ResultsWithEntries returns a Results object from a list of entries
func ResultsWithEntries(q Query, res []Entry) Results {
	b := NewResultBuilder(q)

	// go consume all the entries and add them to the results.
	b.Process.Go(func(worker goprocess.Process) {
		for _, e := range res {
			select {
			case b.Output <- Result{Entry: e}:
			case <-worker.Closing(): // client told us to close early
				return
			}
		}
		return
	})

	go b.Process.CloseAfterChildren()
	return b.Results()
}

func ResultsReplaceQuery(r Results, q Query) Results {
	switch r := r.(type) {
	case *results:
		// note: not using field names to make sure all fields are copied
		return &results{q, r.proc, r.res}
	case *resultsIter:
		// note: not using field names to make sure all fields are copied
		lr := r.legacyResults
		if lr != nil {
			lr = &results{q, lr.proc, lr.res}
		}
		return &resultsIter{q, r.next, r.close, lr}
	default:
		panic("unknown results type")
	}
}

//
// ResultFromIterator provides an alternative way to to construct
// results without the use of channels.
//

func ResultsFromIterator(q Query, iter Iterator) Results {
	if iter.Close == nil {
		iter.Close = noopClose
	}
	return &resultsIter{
		query: q,
		next:  iter.Next,
		close: iter.Close,
	}
}

func noopClose() error {
	return nil
}

type Iterator struct {
	Next  func() (Result, bool)
	Close func() error // note: might be called more than once
}

type resultsIter struct {
	query         Query
	next          func() (Result, bool)
	close         func() error
	legacyResults *results
}

func (r *resultsIter) Next() <-chan Result {
	r.useLegacyResults()
	return r.legacyResults.Next()
}

func (r *resultsIter) NextSync() (Result, bool) {
	if r.legacyResults != nil {
		return r.legacyResults.NextSync()
	} else {
		res, ok := r.next()
		if !ok {
			r.close()
		}
		return res, ok
	}
}

func (r *resultsIter) Rest() ([]Entry, error) {
	var es []Entry
	for {
		e, ok := r.NextSync()
		if !ok {
			break
		}
		if e.Error != nil {
			return es, e.Error
		}
		es = append(es, e.Entry)
	}
	return es, nil
}

func (r *resultsIter) Process() goprocess.Process {
	r.useLegacyResults()
	return r.legacyResults.Process()
}

func (r *resultsIter) Close() error {
	if r.legacyResults != nil {
		return r.legacyResults.Close()
	} else {
		return r.close()
	}
}

func (r *resultsIter) Query() Query {
	return r.query
}

func (r *resultsIter) useLegacyResults() {
	if r.legacyResults != nil {
		return
	}

	b := NewResultBuilder(r.query)

	// go consume all the entries and add them to the results.
	b.Process.Go(func(worker goprocess.Process) {
		defer r.close()
		for {
			e, ok := r.next()
			if !ok {
				break
			}
			select {
			case b.Output <- e:
			case <-worker.Closing(): // client told us to close early
				return
			}
		}
		return
	})

	go b.Process.CloseAfterChildren()

	r.legacyResults = b.Results().(*results)
}
