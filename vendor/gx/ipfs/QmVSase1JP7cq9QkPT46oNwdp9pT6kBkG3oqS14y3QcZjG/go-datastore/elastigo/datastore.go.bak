package elastigo

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"

	"github.com/codahale/blake2"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
)

// Currently, elastigo does not allow connecting to multiple elasticsearch
// instances. The elastigo API uses global static variables (ugh).
// See https://github.com/mattbaird/elastigo/issues/22
//
// Thus, we use a global static variable (GlobalInstance), and return an
// error if NewDatastore is called twice with different addresses.
var GlobalInstance string

// Datastore uses a standard Go map for internal storage.
type Datastore struct {
	url   string
	index string

	// Elastic search does not allow slashes in their object ids,
	// so we hash the key. By default, we use the provided BlakeKeyHash
	KeyHash func(ds.Key) string
}

func NewDatastore(urlstr string) (*Datastore, error) {
	if GlobalInstance != "" && GlobalInstance != urlstr {
		return nil, fmt.Errorf("elastigo only allows one client. See godoc.")
	}

	uf := "http://<host>:<port>/<index>"
	u, err := url.Parse(urlstr)
	if err != nil {
		return nil, fmt.Errorf("error parsing url: %s (%s)", urlstr, uf)
	}

	host := strings.Split(u.Host, ":")
	api.Domain = host[0]
	if len(host) > 1 {
		api.Port = host[1]
	}

	index := strings.Trim(u.Path, "/")
	if strings.Contains(index, "/") {
		e := "elastigo index cannot have slashes: %s (%s -> %s)"
		return nil, fmt.Errorf(e, index, urlstr, uf)
	}

	GlobalInstance = urlstr
	return &Datastore{
		url:     urlstr,
		index:   index,
		KeyHash: BlakeKeyHash,
	}, nil
}

// Returns the ElasticSearch index for given key. If the datastore specifies
// an index, use that. Else, key.Parent
func (d *Datastore) Index(key ds.Key) string {
	if len(d.index) > 0 {
		return d.index
	}
	return key.Parent().BaseNamespace()
}

// value should be JSON serializable.
func (d *Datastore) Put(key ds.Key, value interface{}) (err error) {
	id := d.KeyHash(key)
	res, err := core.Index(false, d.Index(key), key.Type(), id, value)
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return nil
}

func (d *Datastore) Get(key ds.Key) (value interface{}, err error) {
	id := d.KeyHash(key)
	res, err := core.Get(false, d.Index(key), key.Type(), id)
	if err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return res.Source, nil
}

func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	id := d.KeyHash(key)
	return core.Exists(false, d.Index(key), key.Type(), id)
}

func (d *Datastore) Delete(key ds.Key) (err error) {
	id := d.KeyHash(key)
	res, err := core.Delete(false, d.Index(key), key.Type(), id, 0, "")
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("Elasticsearch response: NOT OK. %v", res)
	}
	return nil
}

func (d *Datastore) Query(query.Query) (query.Results, error) {
	return nil, errors.New("Not yet implemented!")
}

// Hash a key and return the first 16 hex chars of its blake2b hash.
// basically: Blake2b(key).HexString[:16]
func BlakeKeyHash(key ds.Key) string {
	h := blake2.NewBlake2B()
	h.Write(key.Bytes())
	d := h.Sum(nil)
	return fmt.Sprintf("%x", d)[:16]
}
