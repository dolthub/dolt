package fsrepo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	repo "github.com/ipfs/go-ipfs/repo"

	measure "gx/ipfs/QmSb95iHExSSb47zpmyn5CyY5PZidVWSjyKyDqgYQrnKor/go-ds-measure"
	flatfs "gx/ipfs/QmUTshC2PP4ZDqkrFfDU4JGJFMWjYnunxPgkQ6ZCA2hGqh/go-ds-flatfs"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	mount "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/syncmount"

	levelds "gx/ipfs/QmPdvXuXWAR6gtxxqZw42RtSADMwz4ijVmYHGS542b6cMz/go-ds-leveldb"
	badgerds "gx/ipfs/QmUamAGkvPp1w84dfc2YMy9ic6iyBvaRoaTiaat8Crtawq/go-ds-badger"
	ldbopts "gx/ipfs/QmbBhyDKsY4mbY6xsKt3qu9Y7FPvMJ6qbD8AMjYYvPRw1g/goleveldb/leveldb/opt"
)

// ConfigFromMap creates a new datastore config from a map
type ConfigFromMap func(map[string]interface{}) (DatastoreConfig, error)

// DatastoreConfig is an abstraction of a datastore config.  A "spec"
// is first converted to a DatastoreConfig and then Create() is called
// to instantiate a new datastore
type DatastoreConfig interface {
	// DiskSpec returns a minimal configuration of the datastore
	// represting what is stored on disk.  Run time values are
	// excluded.
	DiskSpec() DiskSpec

	// Create instantiate a new datastore from this config
	Create(path string) (repo.Datastore, error)
}

// DiskSpec is the type returned by the DatastoreConfig's DiskSpec method
type DiskSpec map[string]interface{}

// Bytes returns a minimal JSON encoding of the DiskSpec
func (spec DiskSpec) Bytes() []byte {
	b, err := json.Marshal(spec)
	if err != nil {
		// should not happen
		panic(err)
	}
	return bytes.TrimSpace(b)
}

// String returns a minimal JSON encoding of the DiskSpec
func (spec DiskSpec) String() string {
	return string(spec.Bytes())
}

var datastores map[string]ConfigFromMap

func init() {
	datastores = map[string]ConfigFromMap{
		"mount":    MountDatastoreConfig,
		"flatfs":   FlatfsDatastoreConfig,
		"levelds":  LeveldsDatastoreConfig,
		"badgerds": BadgerdsDatastoreConfig,
		"mem":      MemDatastoreConfig,
		"log":      LogDatastoreConfig,
		"measure":  MeasureDatastoreConfig,
	}
}

// AnyDatastoreConfig returns a DatastoreConfig from a spec based on
// the "type" parameter
func AnyDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	which, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("'type' field missing or not a string")
	}
	fun, ok := datastores[which]
	if !ok {
		return nil, fmt.Errorf("unknown datastore type: %s", which)
	}
	return fun(params)
}

type mountDatastoreConfig struct {
	mounts []premount
}

type premount struct {
	ds     DatastoreConfig
	prefix ds.Key
}

// MountDatastoreConfig returns a mount DatastoreConfig from a spec
func MountDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var res mountDatastoreConfig
	mounts, ok := params["mounts"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("'mounts' field is missing or not an array")
	}
	for _, iface := range mounts {
		cfg, ok := iface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map for mountpoint")
		}

		child, err := AnyDatastoreConfig(cfg)
		if err != nil {
			return nil, err
		}

		prefix, found := cfg["mountpoint"]
		if !found {
			return nil, fmt.Errorf("no 'mountpoint' on mount")
		}

		res.mounts = append(res.mounts, premount{
			ds:     child,
			prefix: ds.NewKey(prefix.(string)),
		})
	}
	sort.Slice(res.mounts,
		func(i, j int) bool {
			return res.mounts[i].prefix.String() > res.mounts[j].prefix.String()
		})

	return &res, nil
}

func (c *mountDatastoreConfig) DiskSpec() DiskSpec {
	cfg := map[string]interface{}{"type": "mount"}
	mounts := make([]interface{}, len(c.mounts))
	for i, m := range c.mounts {
		c := m.ds.DiskSpec()
		if c == nil {
			c = make(map[string]interface{})
		}
		c["mountpoint"] = m.prefix.String()
		mounts[i] = c
	}
	cfg["mounts"] = mounts
	return cfg
}

func (c *mountDatastoreConfig) Create(path string) (repo.Datastore, error) {
	mounts := make([]mount.Mount, len(c.mounts))
	for i, m := range c.mounts {
		ds, err := m.ds.Create(path)
		if err != nil {
			return nil, err
		}
		mounts[i].Datastore = ds
		mounts[i].Prefix = m.prefix
	}
	return mount.New(mounts), nil
}

type flatfsDatastoreConfig struct {
	path      string
	shardFun  *flatfs.ShardIdV1
	syncField bool
}

// FlatfsDatastoreConfig returns a flatfs DatastoreConfig from a spec
func FlatfsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c flatfsDatastoreConfig
	var ok bool
	var err error

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not boolean")
	}

	sshardFun, ok := params["shardFunc"].(string)
	if !ok {
		return nil, fmt.Errorf("'shardFunc' field is missing or not a string")
	}
	c.shardFun, err = flatfs.ParseShardFunc(sshardFun)
	if err != nil {
		return nil, err
	}

	c.syncField, ok = params["sync"].(bool)
	if !ok {
		return nil, fmt.Errorf("'sync' field is missing or not boolean")
	}
	return &c, nil
}

func (c *flatfsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type":      "flatfs",
		"path":      c.path,
		"shardFunc": c.shardFun.String(),
	}
}

func (c *flatfsDatastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return flatfs.CreateOrOpen(p, c.shardFun, c.syncField)
}

type leveldsDatastoreConfig struct {
	path        string
	compression ldbopts.Compression
}

// LeveldsDatastoreConfig returns a levelds DatastoreConfig from a spec
func LeveldsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c leveldsDatastoreConfig
	var ok bool

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	switch cm := params["compression"].(string); cm {
	case "none":
		c.compression = ldbopts.NoCompression
	case "snappy":
		c.compression = ldbopts.SnappyCompression
	case "":
		c.compression = ldbopts.DefaultCompression
	default:
		return nil, fmt.Errorf("unrecognized value for compression: %s", cm)
	}

	return &c, nil
}

func (c *leveldsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "levelds",
		"path": c.path,
	}
}

func (c *leveldsDatastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return levelds.NewDatastore(p, &levelds.Options{
		Compression: c.compression,
	})
}

type memDatastoreConfig struct {
	cfg map[string]interface{}
}

// MemDatastoreConfig returns a memory DatastoreConfig from a spec
func MemDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	return &memDatastoreConfig{params}, nil
}

func (c *memDatastoreConfig) DiskSpec() DiskSpec {
	return nil
}

func (c *memDatastoreConfig) Create(string) (repo.Datastore, error) {
	return ds.NewMapDatastore(), nil
}

type logDatastoreConfig struct {
	child DatastoreConfig
	name  string
}

// LogDatastoreConfig returns a log DatastoreConfig from a spec
func LogDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	childField, ok := params["child"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'child' field is missing or not a map")
	}
	child, err := AnyDatastoreConfig(childField)
	if err != nil {
		return nil, err
	}
	name, ok := params["name"].(string)
	if !ok {
		return nil, fmt.Errorf("'name' field was missing or not a string")
	}
	return &logDatastoreConfig{child, name}, nil

}

func (c *logDatastoreConfig) Create(path string) (repo.Datastore, error) {
	child, err := c.child.Create(path)
	if err != nil {
		return nil, err
	}
	return ds.NewLogDatastore(child, c.name), nil
}

func (c *logDatastoreConfig) DiskSpec() DiskSpec {
	return c.child.DiskSpec()
}

type measureDatastoreConfig struct {
	child  DatastoreConfig
	prefix string
}

// MeasureDatastoreConfig returns a measure DatastoreConfig from a spec
func MeasureDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	childField, ok := params["child"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'child' field is missing or not a map")
	}
	child, err := AnyDatastoreConfig(childField)
	if err != nil {
		return nil, err
	}
	prefix, ok := params["prefix"].(string)
	if !ok {
		return nil, fmt.Errorf("'prefix' field was missing or not a string")
	}
	return &measureDatastoreConfig{child, prefix}, nil
}

func (c *measureDatastoreConfig) DiskSpec() DiskSpec {
	return c.child.DiskSpec()
}

func (c measureDatastoreConfig) Create(path string) (repo.Datastore, error) {
	child, err := c.child.Create(path)
	if err != nil {
		return nil, err
	}
	return measure.New(c.prefix, child), nil
}

type badgerdsDatastoreConfig struct {
	path       string
	syncWrites bool
}

// BadgerdsDatastoreConfig returns a configuration stub for a badger datastore
// from the given parameters
func BadgerdsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c badgerdsDatastoreConfig
	var ok bool

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	sw, ok := params["syncWrites"]
	if !ok {
		c.syncWrites = true
	} else {
		if swb, ok := sw.(bool); ok {
			c.syncWrites = swb
		} else {
			return nil, fmt.Errorf("'syncWrites' field was not a boolean")
		}
	}

	return &c, nil
}

func (c *badgerdsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "badgerds",
		"path": c.path,
	}
}

func (c *badgerdsDatastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	err := os.MkdirAll(p, 0755)
	if err != nil {
		return nil, err
	}

	defopts := badgerds.DefaultOptions
	defopts.SyncWrites = c.syncWrites

	return badgerds.NewDatastore(p, &defopts)
}
