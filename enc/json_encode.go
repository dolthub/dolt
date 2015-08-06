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

// compoundBlob represents the info needed to encode/decode chunked blob metadata.
type compoundBlob struct {
	length  uint64
	offsets []uint64
	blobs   []ref.Ref
}

func NewCompoundBlob(length uint64, offsets []uint64, refs []ref.Ref) compoundBlob {
	return compoundBlob{length, offsets, refs}
}

// MapFromItems takes an even-numbered list of items and converts them into a stably-ordered map-like value by treating the even-indexed items as keys and the odd-indexed items as values, e.g. {e[0]: e[1], e[2]: e[3], ...}. This does NOT enforce key uniqueness.
func MapFromItems(e ...interface{}) (m encMap) {
	dbg.Chk.True(0 == len(e)%2, "Length on input array must be multiple of 2")
	m = make(encMap, 0, len(e)/2)
	for i := 0; i < len(e); i += 2 {
		m = append(m, mapEntry{e[i], e[i+1]})
	}
	return
}

// SetFromItems turns a list of arbitrary items into a value that will be encoded as a set, but does NOT enforce item uniqueness.
func SetFromItems(e ...interface{}) set {
	return e
}

// encMap holds mapEntries in a stable order at runtime, in contrast to Go maps. This is important so that encoding remains stable.
type encMap []mapEntry

type mapEntry struct {
	k, v interface{}
}

type set []interface{}

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
	case compoundBlob:
		return getJSONCompoundBlob(v)
	case encMap:
		return getJSONMap(v)
	case set:
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

func getJSONCompoundBlob(cb compoundBlob) (interface{}, error) {
	// Perhaps tighten this up: BUG #170
	// {"cb":[{"ref":"sha1-x"},length]}
	// {"cb":[{"ref":"sha1-x"},offset,{"ref":"sha1-y"},length]}
	l := make([]interface{}, 0, len(cb.blobs)*2)
	for i, f := range cb.blobs {
		if i != 0 {
			l = append(l, cb.offsets[i])
		}
		c, err := getJSONPrimitive(f)
		if err != nil {
			return nil, err
		}
		l = append(l, c)
	}
	l = append(l, cb.length)

	dbg.Chk.Equal(len(l), len(cb.blobs)*2)

	return map[string]interface{}{
		"cb": l,
	}, nil
}

func getJSONList(l []interface{}) (r interface{}, err error) {
	j := make([]interface{}, len(l))
	for i, c := range l {
		var cj interface{}
		cj, err = getJSONPrimitive(c)
		if err != nil {
			return
		}
		j[i] = cj
	}
	r = map[string]interface{}{
		"list": j,
	}
	return
}

func getJSONMap(m encMap) (r interface{}, err error) {
	j := make([]interface{}, 0, 2*len(m))
	for _, c := range m {
		var cjk, cjv interface{}
		cjk, err = getJSONPrimitive(c.k)
		if err == nil {
			cjv, err = getJSONPrimitive(c.v)
		}
		if err != nil {
			return nil, err
		}
		j = append(j, cjk)
		j = append(j, cjv)
	}
	r = map[string]interface{}{
		"map": j,
	}
	return
}

func getJSONSet(s set) (r interface{}, err error) {
	j := make([]interface{}, len(s))
	for i, c := range s {
		var cj interface{}
		cj, err = getJSONPrimitive(c)
		if err != nil {
			return
		}
		j[i] = cj
	}
	r = map[string]interface{}{
		"set": j,
	}
	return
}
