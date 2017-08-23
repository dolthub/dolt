package flatfs_test

import (
	"encoding/base32"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gx/ipfs/QmUTshC2PP4ZDqkrFfDU4JGJFMWjYnunxPgkQ6ZCA2hGqh/go-ds-flatfs"
	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	"gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
	dstest "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/test"
)

func tempdir(t testing.TB) (path string, cleanup func()) {
	path, err := ioutil.TempDir("", "test-datastore-flatfs-")
	if err != nil {
		t.Fatalf("cannot create temp directory: %v", err)
	}

	cleanup = func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("tempdir cleanup failed: %v", err)
		}
	}
	return path, cleanup
}

func tryAllShardFuncs(t *testing.T, testFunc func(mkShardFunc, *testing.T)) {
	t.Run("prefix", func(t *testing.T) { testFunc(flatfs.Prefix, t) })
	t.Run("suffix", func(t *testing.T) { testFunc(flatfs.Suffix, t) })
	t.Run("next-to-last", func(t *testing.T) { testFunc(flatfs.NextToLast, t) })
}

func TestPutBadValueType(t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, flatfs.Prefix(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	err = fs.Put(datastore.NewKey("quux"), 22)
	if g, e := err, datastore.ErrInvalidType; g != e {
		t.Fatalf("expected ErrInvalidType, got: %v\n", g)
	}
}

type mkShardFunc func(int) *flatfs.ShardIdV1

func testPut(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	err = fs.Put(datastore.NewKey("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}
}

func TestPut(t *testing.T) { tryAllShardFuncs(t, testPut) }

func testGet(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	const input = "foobar"
	err = fs.Put(datastore.NewKey("quux"), []byte(input))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	data, err := fs.Get(datastore.NewKey("quux"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	buf, ok := data.([]byte)
	if !ok {
		t.Fatalf("expected []byte from Get, got %T: %v", data, data)
	}
	if g, e := string(buf), input; g != e {
		t.Fatalf("Get gave wrong content: %q != %q", g, e)
	}
}

func TestGet(t *testing.T) { tryAllShardFuncs(t, testGet) }

func testPutOverwrite(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	const (
		loser  = "foobar"
		winner = "xyzzy"
	)
	err = fs.Put(datastore.NewKey("quux"), []byte(loser))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	err = fs.Put(datastore.NewKey("quux"), []byte(winner))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	data, err := fs.Get(datastore.NewKey("quux"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if g, e := string(data.([]byte)), winner; g != e {
		t.Fatalf("Get gave wrong content: %q != %q", g, e)
	}
}

func TestPutOverwrite(t *testing.T) { tryAllShardFuncs(t, testPutOverwrite) }

func testGetNotFoundError(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	_, err = fs.Get(datastore.NewKey("quux"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestGetNotFoundError(t *testing.T) { tryAllShardFuncs(t, testGetNotFoundError) }

type params struct {
	shard *flatfs.ShardIdV1
	dir   string
	key   string
}

func testStorage(p *params, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	target := p.dir + string(os.PathSeparator) + p.key + ".data"
	fs, err := flatfs.CreateOrOpen(temp, p.shard, false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	err = fs.Put(datastore.NewKey(p.key), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	seen := false
	haveREADME := false
	walk := func(absPath string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		path, err := filepath.Rel(temp, absPath)
		if err != nil {
			return err
		}
		switch path {
		case ".", "..", "SHARDING":
			// ignore
		case "_README":
			_, err := ioutil.ReadFile(absPath)
			if err != nil {
				t.Error("could not read _README file")
			}
			haveREADME = true
		case p.dir:
			if !fi.IsDir() {
				t.Errorf("directory is not a file? %v", fi.Mode())
			}
			// we know it's there if we see the file, nothing more to
			// do here
		case target:
			seen = true
			if !fi.Mode().IsRegular() {
				t.Errorf("expected a regular file, mode: %04o", fi.Mode())
			}
			if runtime.GOOS != "windows" {
				if g, e := fi.Mode()&os.ModePerm&0007, os.FileMode(0000); g != e {
					t.Errorf("file should not be world accessible: %04o", fi.Mode())
				}
			}
		default:
			t.Errorf("saw unexpected directory entry: %q %v", path, fi.Mode())
		}
		return nil
	}
	if err := filepath.Walk(temp, walk); err != nil {
		t.Fatal("walk: %v", err)
	}
	if !seen {
		t.Error("did not see the data file")
	}
	if fs.ShardStr() == flatfs.IPFS_DEF_SHARD_STR && !haveREADME {
		t.Error("expected _README file")
	} else if fs.ShardStr() != flatfs.IPFS_DEF_SHARD_STR && haveREADME {
		t.Error("did not expect _README file")
	}
}

func TestStorage(t *testing.T) {
	t.Run("prefix", func(t *testing.T) {
		testStorage(&params{
			shard: flatfs.Prefix(2),
			dir:   "qu",
			key:   "quux",
		}, t)
	})
	t.Run("suffix", func(t *testing.T) {
		testStorage(&params{
			shard: flatfs.Suffix(2),
			dir:   "ux",
			key:   "quux",
		}, t)
	})
	t.Run("next-to-last", func(t *testing.T) {
		testStorage(&params{
			shard: flatfs.NextToLast(2),
			dir:   "uu",
			key:   "quux",
		}, t)
	})
}

func testHasNotFound(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	found, err := fs.Has(datastore.NewKey("quux"))
	if err != nil {
		t.Fatalf("Has fail: %v\n", err)
	}
	if g, e := found, false; g != e {
		t.Fatalf("wrong Has: %v != %v", g, e)
	}
}

func TestHasNotFound(t *testing.T) { tryAllShardFuncs(t, testHasNotFound) }

func testHasFound(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	err = fs.Put(datastore.NewKey("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	found, err := fs.Has(datastore.NewKey("quux"))
	if err != nil {
		t.Fatalf("Has fail: %v\n", err)
	}
	if g, e := found, true; g != e {
		t.Fatalf("wrong Has: %v != %v", g, e)
	}
}

func TestHasFound(t *testing.T) { tryAllShardFuncs(t, testHasFound) }

func testDeleteNotFound(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	err = fs.Delete(datastore.NewKey("quux"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected ErrNotFound, got: %v\n", g)
	}
}

func TestDeleteNotFound(t *testing.T) { tryAllShardFuncs(t, testDeleteNotFound) }

func testDeleteFound(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	err = fs.Put(datastore.NewKey("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	err = fs.Delete(datastore.NewKey("quux"))
	if err != nil {
		t.Fatalf("Delete fail: %v\n", err)
	}

	// check that it's gone
	_, err = fs.Get(datastore.NewKey("quux"))
	if g, e := err, datastore.ErrNotFound; g != e {
		t.Fatalf("expected Get after Delete to give ErrNotFound, got: %v\n", g)
	}
}

func TestDeleteFound(t *testing.T) { tryAllShardFuncs(t, testDeleteFound) }

func testQuerySimple(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}
	const myKey = "quux"
	err = fs.Put(datastore.NewKey(myKey), []byte("foobar"))
	if err != nil {
		t.Fatalf("Put fail: %v\n", err)
	}

	res, err := fs.Query(query.Query{KeysOnly: true})
	if err != nil {
		t.Fatalf("Query fail: %v\n", err)
	}
	entries, err := res.Rest()
	if err != nil {
		t.Fatalf("Query Results.Rest fail: %v\n", err)
	}
	seen := false
	for _, e := range entries {
		switch e.Key {
		case datastore.NewKey(myKey).String():
			seen = true
		default:
			t.Errorf("saw unexpected key: %q", e.Key)
		}
	}
	if !seen {
		t.Errorf("did not see wanted key %q in %+v", myKey, entries)
	}
}

func TestQuerySimple(t *testing.T) { tryAllShardFuncs(t, testQuerySimple) }

func testBatchPut(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	dstest.RunBatchTest(t, fs)
}

func TestBatchPut(t *testing.T) { tryAllShardFuncs(t, testBatchPut) }

func testBatchDelete(dirFunc mkShardFunc, t *testing.T) {
	temp, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, dirFunc(2), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	dstest.RunBatchDeleteTest(t, fs)
}

func TestBatchDelete(t *testing.T) { tryAllShardFuncs(t, testBatchDelete) }

func TestSHARDINGFile(t *testing.T) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	fun := flatfs.IPFS_DEF_SHARD

	err := flatfs.Create(tempdir, fun)
	if err != nil {
		t.Fatalf("Create: %v\n", err)
	}

	fs, err := flatfs.Open(tempdir, false)
	if err != nil {
		t.Fatalf("Open fail: %v\n", err)
	}
	if fs.ShardStr() != flatfs.IPFS_DEF_SHARD_STR {
		t.Fatalf("Expected '%s' for shard function got '%s'", flatfs.IPFS_DEF_SHARD_STR, fs.ShardStr())
	}
	fs.Close()

	fs, err = flatfs.CreateOrOpen(tempdir, fun, false)
	if err != nil {
		t.Fatalf("Could not reopen repo: %v\n", err)
	}
	fs.Close()

	fs, err = flatfs.CreateOrOpen(tempdir, flatfs.Prefix(5), false)
	if err == nil {
		t.Fatalf("Was able to open repo with incompatible sharding function")
	}
}

func TestInvalidPrefix(t *testing.T) {
	_, err := flatfs.ParseShardFunc("/bad/prefix/v1/next-to-last/2")
	if err == nil {
		t.Fatalf("Expected an error while parsing a shard identifier with a bad prefix")
	}
}

func TestNonDatastoreDir(t *testing.T) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	ioutil.WriteFile(filepath.Join(tempdir, "afile"), []byte("Some Content"), 0644)

	err := flatfs.Create(tempdir, flatfs.NextToLast(2))
	if err == nil {
		t.Fatalf("Expected an error when creating a datastore in a non-empty directory")
	}
}

func TestNoCluster(t *testing.T) {
	tempdir, cleanup := tempdir(t)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(tempdir, flatfs.NextToLast(1), false)
	if err != nil {
		t.Fatalf("New fail: %v\n", err)
	}

	r := rand.New(rand.NewSource(0))
	N := 3200 // should be divisible by 32 so the math works out
	for i := 0; i < N; i++ {
		blk := make([]byte, 1000)
		r.Read(blk)

		key := "CIQ" + base32.StdEncoding.EncodeToString(blk[:10])
		err := fs.Put(datastore.NewKey(key), blk)
		if err != nil {
			t.Fatalf("Put fail: %v\n", err)
		}
	}

	dirs, err := ioutil.ReadDir(tempdir)
	if err != nil {
		t.Fatalf("ReadDir fail: %v\n", err)
	}
	idealFilesPerDir := float64(N) / 32.0
	tolerance := math.Floor(idealFilesPerDir * 0.25)
	count := 0
	for _, dir := range dirs {
		if dir.Name() == flatfs.SHARDING_FN || dir.Name() == flatfs.README_FN {
			continue
		}
		count += 1
		files, err := ioutil.ReadDir(filepath.Join(tempdir, dir.Name()))
		if err != nil {
			t.Fatalf("ReadDir fail: %v\n", err)
		}
		num := float64(len(files))
		if math.Abs(num-idealFilesPerDir) > tolerance {
			t.Fatalf("Dir %s has %.0f files, expected between %.f and %.f files",
				filepath.Join(tempdir, dir.Name()), num, idealFilesPerDir-tolerance, idealFilesPerDir+tolerance)
		}
	}
	if count != 32 {
		t.Fatalf("Expected 32 directories and one file in %s", tempdir)
	}
}

func BenchmarkConsecutivePut(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var blocks [][]byte
	var keys []datastore.Key
	for i := 0; i < b.N; i++ {
		blk := make([]byte, 256*1024)
		r.Read(blk)
		blocks = append(blocks, blk)

		key := base32.StdEncoding.EncodeToString(blk[:8])
		keys = append(keys, datastore.NewKey(key))
	}
	temp, cleanup := tempdir(b)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, flatfs.Prefix(2), false)
	if err != nil {
		b.Fatalf("New fail: %v\n", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := fs.Put(keys[i], blocks[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchedPut(b *testing.B) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var blocks [][]byte
	var keys []datastore.Key
	for i := 0; i < b.N; i++ {
		blk := make([]byte, 256*1024)
		r.Read(blk)
		blocks = append(blocks, blk)

		key := base32.StdEncoding.EncodeToString(blk[:8])
		keys = append(keys, datastore.NewKey(key))
	}
	temp, cleanup := tempdir(b)
	defer cleanup()

	fs, err := flatfs.CreateOrOpen(temp, flatfs.Prefix(2), false)
	if err != nil {
		b.Fatalf("New fail: %v\n", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; {
		batch, err := fs.Batch()
		if err != nil {
			b.Fatal(err)
		}

		for n := i; i-n < 512 && i < b.N; i++ {
			err := batch.Put(keys[i], blocks[i])
			if err != nil {
				b.Fatal(err)
			}
		}
		err = batch.Commit()
		if err != nil {
			b.Fatal(err)
		}
	}
}
