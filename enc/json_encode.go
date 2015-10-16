package enc

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/attic-labs/noms/d"
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

// CompoundList represents the info needed to encode/decode chunked list metadata.
type CompoundList struct {
	Offsets []uint64 // The offsets of the end of the related lists.
	Lists   []ref.Ref
}

// MapFromItems takes an even-numbered list of items and converts them into a stably-ordered map-like value by treating the even-indexed items as keys and the odd-indexed items as values, e.g. {e[0]: e[1], e[2]: e[3], ...}. This does NOT enforce key uniqueness.
func MapFromItems(e ...interface{}) Map {
	d.Chk.True(0 == len(e)%2, "Length on input array must be multiple of 2")
	return e
}

// SetFromItems turns a list of arbitrary items into a value that will be encoded as a set, but does NOT enforce item uniqueness.
func SetFromItems(e ...interface{}) Set {
	return e
}

// Map holds mapEntries in a stable order at runtime, in contrast to Go maps. This is important so that encoding remains stable.
type Map []interface{}

type Set []interface{}

type TypeRef struct {
	Name string
	Kind uint8
	Desc interface{}
}

func jsonEncode(dst io.Writer, v interface{}) {
	var j interface{}
	j = getJSON(v)

	_, err := dst.Write(jsonTag)
	d.Exp.NoError(err)
	err = json.NewEncoder(dst).Encode(j)
	d.Exp.NoError(err)
	return
}

func getJSON(v interface{}) interface{} {
	switch v := v.(type) {
	case []interface{}:
		return getJSONList(v)
	case CompoundBlob:
		return getJSONCompoundBlob(v)
	case CompoundList:
		return getJSONCompoundList(v)
	case Map:
		return getJSONMap(v)
	case Set:
		return getJSONSet(v)
	default:
		return getJSONPrimitive(v)
	}
}

func getJSONPrimitive(v interface{}) interface{} {
	switch v := v.(type) {
	case bool, string:
		return v
	case float32:
		return map[string]interface{}{
			"float32": float32(v),
		}
	case float64:
		return map[string]interface{}{
			"float64": float64(v),
		}
	case int8:
		return map[string]interface{}{
			"int8": int8(v),
		}
	case int16:
		return map[string]interface{}{
			"int16": int16(v),
		}
	case int32:
		return map[string]interface{}{
			"int32": int32(v),
		}
	case int64:
		return map[string]interface{}{
			"int64": int64(v),
		}
	case ref.Ref:
		return map[string]interface{}{
			"ref": v.String(),
		}
	case uint8:
		return map[string]interface{}{
			"uint8": uint8(v),
		}
	case uint16:
		return map[string]interface{}{
			"uint16": uint16(v),
		}
	case uint32:
		return map[string]interface{}{
			"uint32": uint32(v),
		}
	case uint64:
		return map[string]interface{}{
			"uint64": uint64(v),
		}
	case TypeRef:
		return getJSONTypeRef(v)
	default:
		d.Exp.Fail(fmt.Sprintf("Unexpected type: %T, %+v", v, v))
	}

	return nil // NOTREACHED
}

func getJSONCompoundBlob(cb CompoundBlob) interface{} {
	// {"cb":["sha1-x",length]}
	// {"cb":["sha1-x",lengthX,"sha1-y",lengthY]}
	return getJSONCompoundObject(cb.Blobs, cb.Offsets, "cb")
}

func getJSONCompoundList(cl CompoundList) interface{} {
	// {"cl":["sha1-x",length]}
	// {"cl":["sha1-x",lengthX,"sha1-y",lengthY]}
	return getJSONCompoundObject(cl.Lists, cl.Offsets, "cl")
}

func getJSONCompoundObject(refs []ref.Ref, offsets []uint64, tag string) interface{} {
	// {tag:["sha1-x",lengthX,"sha1-y",lengthY]}
	offset := uint64(0)
	l := make([]interface{}, 0, len(refs)*2)
	for i, r := range refs {
		l = append(l, r.String())
		l = append(l, offsets[i]-offset)
		offset = offsets[i]
	}

	d.Chk.Equal(len(l), len(refs)*2)

	return map[string]interface{}{
		tag: l,
	}
}

func getJSONList(l []interface{}) interface{} {
	return getJSONIterable("list", l)
}

func getJSONMap(m Map) interface{} {
	return getJSONIterable("map", m)
}

func getJSONSet(s Set) interface{} {
	return getJSONIterable("set", s)
}

func getJSONTypeRef(t TypeRef) interface{} {
	body := map[string]interface{}{
		"name": getJSONPrimitive(t.Name),
		"kind": getJSONPrimitive(t.Kind),
	}
	if t.Desc != nil {
		body["desc"] = getJSON(t.Desc)
	}
	return map[string]interface{}{"type": body}
}

func getJSONIterable(tag string, items []interface{}) interface{} {
	j := make([]interface{}, len(items))
	for i, item := range items {
		j[i] = getJSONPrimitive(item)
	}
	return map[string]interface{}{
		tag: j,
	}
}
