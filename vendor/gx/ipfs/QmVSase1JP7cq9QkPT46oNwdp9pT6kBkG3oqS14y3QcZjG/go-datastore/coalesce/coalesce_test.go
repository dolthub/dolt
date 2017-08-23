package coalesce

import (
	"fmt"
	"sync"
	"testing"
	"time"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dscb "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/callback"
	dssync "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/sync"
)

type mock struct {
	sync.Mutex

	inside  int
	outside int
	ds      ds.Datastore
}

func setup() *mock {
	m := &mock{}

	mp := ds.NewMapDatastore()
	ts := dssync.MutexWrap(mp)
	cb1 := dscb.Wrap(ts, func() {
		m.Lock()
		m.inside++
		m.Unlock()
		<-time.After(20 * time.Millisecond)
	})
	cd := Wrap(cb1)
	cb2 := dscb.Wrap(cd, func() {
		m.Lock()
		m.outside++
		m.Unlock()
	})

	m.ds = cb2
	return m
}

func TestCoalesceSamePut(t *testing.T) {
	m := setup()
	done := make(chan struct{})

	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar")
		done <- struct{}{}
	}()
	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar")
		done <- struct{}{}
	}()
	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar")
		done <- struct{}{}
	}()

	<-done
	<-done
	<-done

	if m.inside != 1 {
		t.Error("incalls should be 1", m.inside)
	}

	if m.outside != 3 {
		t.Error("outcalls should be 3", m.outside)
	}
}

func TestCoalesceSamePutDiffPut(t *testing.T) {
	m := setup()
	done := make(chan struct{})

	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar")
		done <- struct{}{}
	}()
	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar")
		done <- struct{}{}
	}()
	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar2")
		done <- struct{}{}
	}()
	go func() {
		m.ds.Put(ds.NewKey("foo"), "bar3")
		done <- struct{}{}
	}()

	<-done
	<-done
	<-done
	<-done

	if m.inside != 3 {
		t.Error("incalls should be 3", m.inside)
	}

	if m.outside != 4 {
		t.Error("outcalls should be 4", m.outside)
	}
}

func TestCoalesceSameGet(t *testing.T) {
	m := setup()
	done := make(chan struct{})
	errs := make(chan error, 30)

	m.ds.Put(ds.NewKey("foo1"), "bar")
	m.ds.Put(ds.NewKey("foo2"), "baz")

	for i := 0; i < 10; i++ {
		go func() {
			v, err := m.ds.Get(ds.NewKey("foo1"))
			if err != nil {
				errs <- err
			}
			if v != "bar" {
				errs <- fmt.Errorf("v is not bar", v)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			v, err := m.ds.Get(ds.NewKey("foo2"))
			if err != nil {
				errs <- err
			}
			if v != "baz" {
				errs <- fmt.Errorf("v is not baz", v)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			_, err := m.ds.Get(ds.NewKey("foo3"))
			if err == nil {
				errs <- fmt.Errorf("no error")
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 30; i++ {
		<-done
	}

	if m.inside != 5 {
		t.Error("incalls should be 3", m.inside)
	}

	if m.outside != 32 {
		t.Error("outcalls should be 30", m.outside)
	}
}

func TestCoalesceHas(t *testing.T) {
	m := setup()
	done := make(chan struct{})
	errs := make(chan error, 30)

	m.ds.Put(ds.NewKey("foo1"), "bar")
	m.ds.Put(ds.NewKey("foo2"), "baz")

	for i := 0; i < 10; i++ {
		go func() {
			v, err := m.ds.Has(ds.NewKey("foo1"))
			if err != nil {
				errs <- err
			}
			if !v {
				errs <- fmt.Errorf("should have foo1")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			v, err := m.ds.Has(ds.NewKey("foo2"))
			if err != nil {
				errs <- err
			}
			if !v {
				errs <- fmt.Errorf("should have foo2")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			v, err := m.ds.Has(ds.NewKey("foo3"))
			if err != nil {
				errs <- err
			}
			if v {
				errs <- fmt.Errorf("should not have foo3")
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 30; i++ {
		<-done
	}

	if m.inside != 5 {
		t.Error("incalls should be 3", m.inside)
	}

	if m.outside != 32 {
		t.Error("outcalls should be 30", m.outside)
	}
}

func TestCoalesceDelete(t *testing.T) {
	m := setup()
	done := make(chan struct{})
	errs := make(chan error, 30)

	m.ds.Put(ds.NewKey("foo1"), "bar1")
	m.ds.Put(ds.NewKey("foo2"), "bar2")
	m.ds.Put(ds.NewKey("foo3"), "bar3")

	for i := 0; i < 10; i++ {
		go func() {
			err := m.ds.Delete(ds.NewKey("foo1"))
			if err != nil {
				errs <- err
			}
			has, err := m.ds.Has(ds.NewKey("foo1"))
			if err != nil {
				errs <- err
			}
			if has {
				t.Error("still have it after deleting")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			err := m.ds.Delete(ds.NewKey("foo2"))
			if err != nil {
				errs <- err
			}
			has, err := m.ds.Has(ds.NewKey("foo2"))
			if err != nil {
				errs <- err
			}
			if has {
				t.Error("still have it after deleting")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			has, err := m.ds.Has(ds.NewKey("foo3"))
			if err != nil {
				errs <- err
			}
			if !has {
				t.Error("should still have foo3")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		go func() {
			has, err := m.ds.Has(ds.NewKey("foo4"))
			if err != nil {
				errs <- err
			}
			if has {
				t.Error("should not have foo4")
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 40; i++ {
		<-done
	}

	if m.inside != 9 {
		t.Error("incalls should be 9", m.inside)
	}

	if m.outside != 63 {
		t.Error("outcalls should be 63", m.outside)
	}
}
