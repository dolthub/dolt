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
	length       uint64
	childLengths []uint64
	blobs        []ref.Ref
}

// Map holds mapEntries in a stable order at runtime, in contrast to Go maps. This is important so that encoding remains stable.
type Map []mapEntry

type mapEntry struct {
	k, v interface{}
}

// Set represents (but does not in any way enforce) a list of unique items.
type Set []interface{}

// MapFromItems takes an even-numbered list of items and converts them into a Map by treating the even-indexed items as keys and the odd-indexed items as values, e.g. {e[0]: e[1], e[2]: e[3], ...}
func MapFromItems(e ...interface{}) (m Map) {
	dbg.Chk.True(0 == len(e)%2, "Length on input array must be multiple of 2")
	m = make(Map, 0, len(e)/2)
	for i := 0; i < len(e); i += 2 {
		m = append(m, mapEntry{e[i], e[i+1]})
	}
	return
}

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
	var err error
	l := make([]interface{}, len(cb.blobs)*2+1)
	l[0] = cb.length
	for i, f := range cb.blobs {
		l[i*2+1] = cb.childLengths[i]
		if l[i*2+2], err = getJSONPrimitive(f); err != nil {
			return nil, err
		}
	}

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

func getJSONMap(m Map) (r interface{}, err error) {
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

func getJSONSet(s Set) (r interface{}, err error) {
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
