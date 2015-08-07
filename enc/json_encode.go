package enc

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var (
	jsonTag = []byte("j ")
)

// CompoundBlob represents the info needed to encode/decode chunked blob metadata.
type CompoundBlob struct {
	Offsets []uint64 // The offsets of the end of the related blobs.
	Blobs   []ref.Ref
}

func (cb CompoundBlob) Len() uint64 {
	return cb.Offsets[len(cb.Offsets)-1]
}

// MapFromItems takes an even-numbered list of items and converts them into a stably-ordered map-like value by treating the even-indexed items as keys and the odd-indexed items as values, e.g. {e[0]: e[1], e[2]: e[3], ...}. This does NOT enforce key uniqueness.
func MapFromItems(e ...interface{}) Map {
	dbg.Chk.True(0 == len(e)%2, "Length on input array must be multiple of 2")
	return e
}

// SetFromItems turns a list of arbitrary items into a value that will be encoded as a set, but does NOT enforce item uniqueness.
func SetFromItems(e ...interface{}) Set {
	return e
}

// Map holds mapEntries in a stable order at runtime, in contrast to Go maps. This is important so that encoding remains stable.
type Map []interface{}

type Set []interface{}

func jsonEncode(dst io.Writer, v interface{}) (err error) {
	var j interface{}
	j, err = getJSON(v)
	if err != nil {
		return
	}
	_, err = dst.Write(jsonTag)
	if err == nil {
		err = json.NewEncoder(dst).Encode(j)
	}
	return
}

func getJSON(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case []interface{}:
		return getJSONList(v)
	case CompoundBlob:
		return getJSONCompoundBlob(v)
	case Map:
		return getJSONMap(v)
	case Set:
		return getJSONSet(v)
	default:
		return getJSONPrimitive(v)
	}
}

func getJSONPrimitive(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case bool, string:
		return v, nil
	case float32:
		return map[string]interface{}{
			"float32": float32(v),
		}, nil
	case float64:
		return map[string]interface{}{
			"float64": float64(v),
		}, nil
	case int16:
		return map[string]interface{}{
			"int16": int16(v),
		}, nil
	case int32:
		return map[string]interface{}{
			"int32": int32(v),
		}, nil
	case int64:
		return map[string]interface{}{
			"int64": int64(v),
		}, nil
	case ref.Ref:
		return map[string]interface{}{
			"ref": v.String(),
		}, nil
	case uint16:
		return map[string]interface{}{
			"uint16": uint16(v),
		}, nil
	case uint32:
		return map[string]interface{}{
			"uint32": uint32(v),
		}, nil
	case uint64:
		return map[string]interface{}{
			"uint64": uint64(v),
		}, nil
	default:
		panic(fmt.Sprintf("Unexpected type: %T, %+v", v, v))
	}
}

func getJSONCompoundBlob(cb CompoundBlob) (interface{}, error) {
	// Perhaps tighten this up: BUG #170
	// {"cb":[{"ref":"sha1-x"},length]}
	// {"cb":[{"ref":"sha1-x"},lengthX,{"ref":"sha1-y"},lengthY]}
	offset := uint64(0)
	l := make([]interface{}, 0, len(cb.Blobs)*2)
	for i, f := range cb.Blobs {
		c, err := getJSONPrimitive(f)
		if err != nil {
			return nil, err
		}
		l = append(l, c)
		l = append(l, cb.Offsets[i]-offset)
		offset = cb.Offsets[i]
	}

	dbg.Chk.Equal(len(l), len(cb.Blobs)*2)

	return map[string]interface{}{
		"cb": l,
	}, nil
}

func getJSONList(l []interface{}) (r interface{}, err error) {
	return getJSONIterable("list", l)
}

func getJSONMap(m Map) (r interface{}, err error) {
	return getJSONIterable("map", m)
}

func getJSONSet(s Set) (r interface{}, err error) {
	return getJSONIterable("set", s)
}

func getJSONIterable(tag string, items []interface{}) (r interface{}, err error) {
	j := make([]interface{}, len(items))
	for i, item := range items {
		var json interface{}
		json, err = getJSONPrimitive(item)
		if err != nil {
			return nil, err
		}
		j[i] = json
	}
	r = map[string]interface{}{
		tag: j,
	}
	return
}
