package autoincr

import (
	"sync"
)

type RefInMemStore interface {
	GetRefMemStore(refhash string) *refMemStore
	SetRefMemStore(refhash string, store *refMemStore)
}

func NewSessionGlobalInMemStore() RefInMemStore {
	return &refInMemStoreImpl{
		refToMemStore: make(map[string]*refMemStore),
	}
}

type refMemStore struct {
	Ait AutoIncrementTracker
}

type refInMemStoreImpl struct {
	refToMemStore map[string]*refMemStore
	mu            sync.Mutex
}

var _ RefInMemStore = (*refInMemStoreImpl)(nil)

func (s *refInMemStoreImpl) GetRefMemStore(refhash string) *refMemStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	memStore, ok := s.refToMemStore[refhash]
	if !ok {
		memStore = &refMemStore{
			Ait: NewAutoIncrementTracker(),
		}
		s.refToMemStore[refhash] = memStore
	}

	return memStore
}

func (s *refInMemStoreImpl) SetRefMemStore(refhash string, store *refMemStore) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refToMemStore[refhash] = store
}
