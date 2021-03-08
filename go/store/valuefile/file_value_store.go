package valuefile

import (
	"context"
	"sort"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var _ chunks.ChunkStore = (*FileValueStore)(nil)
var _ types.ValueReadWriter = (*FileValueStore)(nil)

type FileValueStore struct {
	nbf *types.NomsBinFormat

	valLock *sync.Mutex
	values  map[hash.Hash]types.Value

	rootHash  hash.Hash
	chunkLock *sync.Mutex
	chunks    map[hash.Hash][]byte
}

func NewFileValueStore(nbf *types.NomsBinFormat) (*FileValueStore, error) {
	return &FileValueStore{
		nbf:       nbf,
		valLock:   &sync.Mutex{},
		values:    make(map[hash.Hash]types.Value),
		chunkLock: &sync.Mutex{},
		chunks:    make(map[hash.Hash][]byte),
	}, nil
}

func (f *FileValueStore) Format() *types.NomsBinFormat {
	return f.nbf
}

func (f *FileValueStore) ReadValue(ctx context.Context, h hash.Hash) (types.Value, error) {
	f.valLock.Lock()
	defer f.valLock.Unlock()

	v := f.values[h]
	return v, nil
}

func (f *FileValueStore) ReadManyValues(ctx context.Context, hashes hash.HashSlice) (types.ValueSlice, error) {
	f.valLock.Lock()
	defer f.valLock.Unlock()

	vals := make(types.ValueSlice, len(hashes))
	for i, h := range hashes {
		vals[i] = f.values[h]
	}

	return vals, nil
}

func (f *FileValueStore) WriteValue(ctx context.Context, v types.Value) (types.Ref, error) {
	f.valLock.Lock()
	defer f.valLock.Unlock()

	h, err := v.Hash(f.nbf)

	if err != nil {
		return types.Ref{}, err
	}

	f.values[h] = v

	c, err := types.EncodeValue(v, f.nbf)

	if err != nil {
		return types.Ref{}, err
	}

	err = f.Put(ctx, c)

	if err != nil {
		return types.Ref{}, err
	}

	return types.NewRef(v, f.nbf)
}

func (f *FileValueStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	data, ok := f.chunks[h]

	if !ok {
		return chunks.EmptyChunk, nil
	} else {
		return chunks.NewChunkWithHash(h, data), nil
	}
}

func (f *FileValueStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(*chunks.Chunk)) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	for h := range hashes {
		data, ok := f.chunks[h]

		if ok {
			ch := chunks.NewChunkWithHash(h, data)
			found(&ch)
		}
	}

	return nil
}

func (f *FileValueStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	_, ok := f.chunks[h]
	return ok, nil
}

func (f *FileValueStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	absent = make(hash.HashSet, len(hashes))
	for h := range hashes {
		_, ok := f.chunks[h]

		if !ok {
			absent[h] = struct{}{}
		}
	}

	return absent, nil
}

func (f *FileValueStore) Put(ctx context.Context, c chunks.Chunk) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	f.chunks[c.Hash()] = c.Data()
	return nil
}

func (f *FileValueStore) Version() string {
	return f.nbf.VersionString()
}

func (f *FileValueStore) Rebase(ctx context.Context) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()
	return nil
}

func (f *FileValueStore) Root(ctx context.Context) (hash.Hash, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()
	return f.rootHash, nil
}

func (f *FileValueStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	if f.rootHash == last {
		f.rootHash = current
		return true, nil
	}

	return false, nil
}

func (f *FileValueStore) Stats() interface{} {
	return nil
}

func (f *FileValueStore) StatsSummary() string {
	return ""
}

func (f *FileValueStore) Close() error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	return nil
}

func (f *FileValueStore) numChunks() int {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	return len(f.chunks)
}

func (f *FileValueStore) iterChunks(cb func(ch chunks.Chunk) error) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	hashes := make(hash.HashSlice, 0, len(f.chunks))
	for h := range f.chunks {
		hashes = append(hashes, h)
	}

	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].Less(hashes[j])
	})

	for _, h := range hashes {
		data := f.chunks[h]
		err := cb(chunks.NewChunkWithHash(h, data))

		if err != nil {
			return err
		}
	}

	return nil
}
