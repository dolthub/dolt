package fsrepo

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-ipfs/repo/config"
	"github.com/ipfs/go-ipfs/thirdparty/assert"
	datastore "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

// swap arg order
func testRepoPath(p string, t *testing.T) string {
	name, err := ioutil.TempDir("", p)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func TestInitIdempotence(t *testing.T) {
	t.Parallel()
	path := testRepoPath("", t)
	for i := 0; i < 10; i++ {
		assert.Nil(Init(path, &config.Config{Datastore: config.DefaultDatastoreConfig()}), t, "multiple calls to init should succeed")
	}
}

func Remove(repoPath string) error {
	repoPath = filepath.Clean(repoPath)
	return os.RemoveAll(repoPath)
}

func TestCanManageReposIndependently(t *testing.T) {
	t.Parallel()
	pathA := testRepoPath("a", t)
	pathB := testRepoPath("b", t)

	t.Log("initialize two repos")
	assert.Nil(Init(pathA, &config.Config{Datastore: config.DefaultDatastoreConfig()}), t, "a", "should initialize successfully")
	assert.Nil(Init(pathB, &config.Config{Datastore: config.DefaultDatastoreConfig()}), t, "b", "should initialize successfully")

	t.Log("ensure repos initialized")
	assert.True(IsInitialized(pathA), t, "a should be initialized")
	assert.True(IsInitialized(pathB), t, "b should be initialized")

	t.Log("open the two repos")
	repoA, err := Open(pathA)
	assert.Nil(err, t, "a")
	repoB, err := Open(pathB)
	assert.Nil(err, t, "b")

	t.Log("close and remove b while a is open")
	assert.Nil(repoB.Close(), t, "close b")
	assert.Nil(Remove(pathB), t, "remove b")

	t.Log("close and remove a")
	assert.Nil(repoA.Close(), t)
	assert.Nil(Remove(pathA), t)
}

func TestDatastoreGetNotAllowedAfterClose(t *testing.T) {
	t.Parallel()
	path := testRepoPath("test", t)

	assert.True(!IsInitialized(path), t, "should NOT be initialized")
	assert.Nil(Init(path, &config.Config{Datastore: config.DefaultDatastoreConfig()}), t, "should initialize successfully")
	r, err := Open(path)
	assert.Nil(err, t, "should open successfully")

	k := "key"
	data := []byte(k)
	assert.Nil(r.Datastore().Put(datastore.NewKey(k), data), t, "Put should be successful")

	assert.Nil(r.Close(), t)
	_, err = r.Datastore().Get(datastore.NewKey(k))
	assert.Err(err, t, "after closer, Get should be fail")
}

func TestDatastorePersistsFromRepoToRepo(t *testing.T) {
	t.Parallel()
	path := testRepoPath("test", t)

	assert.Nil(Init(path, &config.Config{Datastore: config.DefaultDatastoreConfig()}), t)
	r1, err := Open(path)
	assert.Nil(err, t)

	k := "key"
	expected := []byte(k)
	assert.Nil(r1.Datastore().Put(datastore.NewKey(k), expected), t, "using first repo, Put should be successful")
	assert.Nil(r1.Close(), t)

	r2, err := Open(path)
	assert.Nil(err, t)
	v, err := r2.Datastore().Get(datastore.NewKey(k))
	assert.Nil(err, t, "using second repo, Get should be successful")
	actual, ok := v.([]byte)
	assert.True(ok, t, "value should be the []byte from r1's Put")
	assert.Nil(r2.Close(), t)
	assert.True(bytes.Equal(expected, actual), t, "data should match")
}

func TestOpenMoreThanOnceInSameProcess(t *testing.T) {
	t.Parallel()
	path := testRepoPath("", t)
	assert.Nil(Init(path, &config.Config{Datastore: config.DefaultDatastoreConfig()}), t)

	r1, err := Open(path)
	assert.Nil(err, t, "first repo should open successfully")
	r2, err := Open(path)
	assert.Nil(err, t, "second repo should open successfully")
	assert.True(r1 == r2, t, "second open returns same value")

	assert.Nil(r1.Close(), t)
	assert.Nil(r2.Close(), t)
}
