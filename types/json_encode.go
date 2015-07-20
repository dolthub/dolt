package types

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

var (
	jsonTag = []byte("j ")
)

func jsonEncode(v Value, s chunks.ChunkSink) (r ref.Ref, err error) {
	var j interface{}
	j, err = getJSON(v, s)
	if err != nil {
		return
	}
	w := s.Put()
	_, err = w.Write(jsonTag)
	if err == nil {
		err = json.NewEncoder(w).Encode(j)
	}
	if err != nil {
		return
	}
	return w.Ref()
}

func getJSON(v Value, s chunks.ChunkSink) (interface{}, error) {
	switch v := v.(type) {
	case Blob:
		Chk.Fail(fmt.Sprintf("jsonEncode doesn't support encoding blobs - didn't expect to get here: %+v", v))
	case Bool:
		return bool(v), nil
	case Float32:
		return map[string]interface{}{
			"float32": float32(v),
		}, nil
	case Float64:
		return map[string]interface{}{
			"float64": float64(v),
		}, nil
	case Int16:
		return map[string]interface{}{
			"int16": int16(v),
		}, nil
	case Int32:
		return map[string]interface{}{
			"int32": int32(v),
		}, nil
	case Int64:
		return map[string]interface{}{
			"int64": int64(v),
		}, nil
	case List:
		return getJSONList(v, s)
	case Map:
		return getJSONMap(v, s)
	case Set:
		return getJSONSet(v, s)
	case String:
		return v.String(), nil
	case UInt16:
		return map[string]interface{}{
			"uint16": uint16(v),
		}, nil
	case UInt32:
		return map[string]interface{}{
			"uint32": uint32(v),
		}, nil
	case UInt64:
		return map[string]interface{}{
			"uint64": uint64(v),
		}, nil
	default:
		panic(fmt.Sprintf("Unexpected type: %+v", v))
	}
	return nil, nil
}

func getJSONList(l List, s chunks.ChunkSink) (r interface{}, err error) {
	j := []interface{}{}
	for i := uint64(0); i < l.Len(); i++ {
		var cj interface{}
		cj, err = getChildJSON(l.list[i], s)
		if err != nil {
			return
		}
		j = append(j, cj)
	}
	r = map[string]interface{}{
		"list": j,
	}
	return
}

type entrySlice []mapEntry

func (mes entrySlice) Len() int {
	return len(mes)
}

func (mes entrySlice) Swap(i, j int) {
	mes[i], mes[j] = mes[j], mes[i]
}

func (mes entrySlice) Less(i, j int) bool {
	return ref.Less(mes[i].key.Ref(), mes[j].key.Ref())
}

func getJSONMap(m Map, s chunks.ChunkSink) (r interface{}, err error) {
	j := []interface{}{}
	for _, r := range m.m {
		var cjk, cjv interface{}
		cjk, err = getChildJSON(r.key, s)
		if err == nil {
			cjv, err = getChildJSON(r.value, s)
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

func getJSONSet(set Set, s chunks.ChunkSink) (r interface{}, err error) {
	// Iteration through Set is random, but we need a deterministic order for serialization. Let's order using the refs of the values in the set.
	lookup := setData{}
	order := ref.RefSlice{}
	for _, f := range set.m {
		order = append(order, f.Ref())
		lookup[f.Ref()] = f
	}
	sort.Sort(order)

	j := []interface{}{}
	for _, r := range order {
		f := lookup[r]
		var cj interface{}
		cj, err = getChildJSON(f, s)
		if err != nil {
			return nil, err
		}
		j = append(j, cj)
	}

	r = map[string]interface{}{
		"set": j,
	}
	return
}

func getChildJSON(f future, s chunks.ChunkSink) (interface{}, error) {
	var r ref.Ref
	var err error
	if v, ok := f.(*unresolvedFuture); ok {
		r = v.Ref()
	} else {
		v := f.Val()
		Chk.NotNil(v)
		switch v := v.(type) {
		// Blobs, lists, maps, and sets are always out-of-line
		case Blob:
			r, err = WriteValue(v, s)
		case List:
			r, err = WriteValue(v, s)
		case Map:
			r, err = WriteValue(v, s)
		case Set:
			r, err = WriteValue(v, s)
		default:
			// Other types are always inline.
			return getJSON(v, s)
		}
		if err != nil {
			return nil, err
		}
	}
	Chk.NotNil(r)
	return map[string]interface{}{
		"ref": r.String(),
	}, nil
}
