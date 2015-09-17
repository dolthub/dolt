package chunks

import (
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/suite"
)

func TestReadThroughStoreTestSuite(t *testing.T) {
	suite.Run(t, &ReadThroughStoreTestSuite{})
}

type ReadThroughStoreTestSuite struct {
	ChunkStoreTestSuite
}

func (suite *ReadThroughStoreTestSuite) SetupTest() {
	suite.Store = NewReadThroughStore(NewMemoryStore(), NewTestStore())
}

func (suite *ReadThroughStoreTestSuite) TearDownTest() {
	suite.Store.Close()
}

func (suite *LevelDBStoreTestSuite) TestReadThroughStoreGet() {
	bs := NewTestStore()

	// Prepopulate the backing store with "abc".
	input := "abc"
	w := bs.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	suite.Equal(1, bs.Len())
	suite.Equal(1, bs.Writes)
	suite.Equal(0, bs.Reads)

	cs := NewTestStore()
	rts := NewReadThroughStore(cs, bs)

	// Now read "abc". It is not yet in the cache so we hit the backing store.
	data := rts.Get(ref)
	suite.Equal(input, string(data))

	suite.Equal(1, bs.Len())
	suite.Equal(1, cs.Len())
	suite.Equal(1, cs.Writes)
	suite.Equal(1, bs.Writes)
	suite.Equal(1, cs.Reads)
	suite.Equal(1, bs.Reads)

	// Reading it again should not hit the backing store.
	data = rts.Get(ref)
	suite.Equal(input, string(data))

	suite.Equal(1, bs.Len())
	suite.Equal(1, cs.Len())
	suite.Equal(1, cs.Writes)
	suite.Equal(1, bs.Writes)
	suite.Equal(2, cs.Reads)
	suite.Equal(1, bs.Reads)
}

func (suite *LevelDBStoreTestSuite) TestReadThroughStorePut() {
	bs := NewTestStore()
	cs := NewTestStore()
	rts := NewReadThroughStore(cs, bs)

	// Storing "abc" should store it to both backing and caching store.
	input := "abc"
	w := rts.Put()
	_, err := w.Write([]byte(input))
	suite.NoError(err)
	ref := w.Ref()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal("sha1-a9993e364706816aba3e25717850c26c9cd0d89d", ref.String())

	assertInputInStore("abc", ref, bs, suite.Assert())
	assertInputInStore("abc", ref, cs, suite.Assert())
	assertInputInStore("abc", ref, rts, suite.Assert())
}
