package blobstore

import (
	"bytes"
	"io"
	"io/ioutil"
)

// Blobstore is an interface for storing and retrieving blobs of data by key
type Blobstore interface {
	Exists(key string) (bool, error)
	Get(key string, br BlobRange) (io.ReadCloser, string, error)
	Put(key string, reader io.Reader) (string, error)
	CheckAndPut(expectedVersion, key string, reader io.Reader) (string, error)
}

// GetBytes is a utility method calls bs.Get and handles reading the data from the returned
// io.ReadCloser and closing it.
func GetBytes(bs Blobstore, key string, br BlobRange) ([]byte, string, error) {
	rc, ver, err := bs.Get(key, br)

	if err != nil || rc == nil {
		return nil, ver, err
	}

	defer rc.Close()
	data, err := ioutil.ReadAll(rc)

	if err != nil {
		return nil, "", err
	}

	return data, ver, nil
}

// PutBytes is a utility method calls bs.Put by wrapping the supplied []byte in an io.Reader
func PutBytes(bs Blobstore, key string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	return bs.Put(key, reader)
}

// CheckAndPutBytes is a utility method calls bs.CheckAndPut by wrapping the supplied []byte
// in an io.Reader
func CheckAndPutBytes(bs Blobstore, expectedVersion, key string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	return bs.CheckAndPut(expectedVersion, key, reader)
}
