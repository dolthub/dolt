package fsrepo

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	config "github.com/ipfs/go-ipfs/repo/config"
)

// note: to test sorting of the mountpoints in the disk spec they are
// specified out of order in the test config
var defaultConfig = []byte(`{
    "StorageMax": "10GB",
    "StorageGCWatermark": 90,
    "GCPeriod": "1h",
    "Spec": {
      "mounts": [
        {
          "child": {
            "compression": "none",
            "path": "datastore",
            "type": "levelds"
          },
          "mountpoint": "/",
          "prefix": "leveldb.datastore",
          "type": "measure"
        },
        {
          "child": {
            "path": "blocks",
            "shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
            "sync": true,
            "type": "flatfs"
          },
          "mountpoint": "/blocks",
          "prefix": "flatfs.datastore",
          "type": "measure"
        }
      ],
      "type": "mount"
    },
    "HashOnRead": false,
    "BloomFilterSize": 0
}`)

var leveldbConfig = []byte(`{
            "compression": "none",
            "path": "datastore",
            "type": "levelds"
}`)

var flatfsConfig = []byte(`{
            "path": "blocks",
            "shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
            "sync": true,
            "type": "flatfs"
}`)

var measureConfig = []byte(`{
          "child": {
            "path": "blocks",
            "shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
            "sync": true,
            "type": "flatfs"
          },
          "mountpoint": "/blocks",
          "prefix": "flatfs.datastore",
          "type": "measure"
}`)

func TestDefaultDatastoreConfig(t *testing.T) {
	dir, err := ioutil.TempDir("", "ipfs-datastore-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	config := new(config.Datastore)
	err = json.Unmarshal(defaultConfig, config)
	if err != nil {
		t.Fatal(err)
	}

	dsc, err := AnyDatastoreConfig(config.Spec)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","type":"flatfs"},{"mountpoint":"/","path":"datastore","type":"levelds"}],"type":"mount"}`
	if dsc.DiskSpec().String() != expected {
		t.Errorf("expected '%s' got '%s' as DiskId", expected, dsc.DiskSpec().String())
	}

	ds, err := dsc.Create(dir)
	if err != nil {
		t.Fatal(err)
	}

	if typ := reflect.TypeOf(ds).String(); typ != "*syncmount.Datastore" {
		t.Errorf("expected '*syncmount.Datastore' got '%s'", typ)
	}
}

func TestLevelDbConfig(t *testing.T) {
	config := new(config.Datastore)
	err := json.Unmarshal(defaultConfig, config)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := ioutil.TempDir("", "ipfs-datastore-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	spec := make(map[string]interface{})
	err = json.Unmarshal(leveldbConfig, &spec)
	if err != nil {
		t.Fatal(err)
	}

	dsc, err := AnyDatastoreConfig(spec)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"path":"datastore","type":"levelds"}`
	if dsc.DiskSpec().String() != expected {
		t.Errorf("expected '%s' got '%s' as DiskId", expected, dsc.DiskSpec().String())
	}

	ds, err := dsc.Create(dir)
	if err != nil {
		t.Fatal(err)
	}

	if typ := reflect.TypeOf(ds).String(); typ != "*leveldb.datastore" {
		t.Errorf("expected '*leveldb.datastore' got '%s'", typ)
	}
}

func TestFlatfsConfig(t *testing.T) {
	config := new(config.Datastore)
	err := json.Unmarshal(defaultConfig, config)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := ioutil.TempDir("", "ipfs-datastore-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	spec := make(map[string]interface{})
	err = json.Unmarshal(flatfsConfig, &spec)
	if err != nil {
		t.Fatal(err)
	}

	dsc, err := AnyDatastoreConfig(spec)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","type":"flatfs"}`
	if dsc.DiskSpec().String() != expected {
		t.Errorf("expected '%s' got '%s' as DiskId", expected, dsc.DiskSpec().String())
	}

	ds, err := dsc.Create(dir)
	if err != nil {
		t.Fatal(err)
	}

	if typ := reflect.TypeOf(ds).String(); typ != "*flatfs.Datastore" {
		t.Errorf("expected '*flatfs.Datastore' got '%s'", typ)
	}
}

func TestMeasureConfig(t *testing.T) {
	config := new(config.Datastore)
	err := json.Unmarshal(defaultConfig, config)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := ioutil.TempDir("", "ipfs-datastore-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	spec := make(map[string]interface{})
	err = json.Unmarshal(measureConfig, &spec)
	if err != nil {
		t.Fatal(err)
	}

	dsc, err := AnyDatastoreConfig(spec)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","type":"flatfs"}`
	if dsc.DiskSpec().String() != expected {
		t.Errorf("expected '%s' got '%s' as DiskId", expected, dsc.DiskSpec().String())
	}

	ds, err := dsc.Create(dir)
	if err != nil {
		t.Fatal(err)
	}

	if typ := reflect.TypeOf(ds).String(); typ != "*measure.measure" {
		t.Errorf("expected '*measure.measure' got '%s'", typ)
	}
}
