// gob.go - Encode/Decode a Map into a gob object.

package mxj

import (
	"bytes"
	"encoding/gob"
)

// NewMapGob returns a Map value for a gob object that has been
// encoded from a map[string]interface{} (or compatible type) value.
// It is intended to provide symmetric handling of Maps that have
// been encoded using mv.Gob.
func NewMapGob(gobj []byte) (Map, error) {
	m := make(map[string]interface{}, 0)
	if len(gobj) == 0 {
		return m, nil
	}
	r := bytes.NewReader(gobj)
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&m); err != nil {
		return m, err
	}
	return m, nil
}

// Gob returns a gob-encoded value for the Map 'mv'.
func (mv Map) Gob() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(map[string]interface{}(mv)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
