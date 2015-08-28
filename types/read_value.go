package types

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

// ReadValue reads and decodes a value from a chunk source. It is not considered an error for the requested chunk to be absent from cs; in this case, the function simply returns nil, nil.
func ReadValue(r ref.Ref, cs chunks.ChunkSource) Value {
	d.Chk.NotNil(cs)
	reader := cs.Get(r)
	if reader == nil {
		return nil
	}
	defer reader.Close()

	i := enc.Decode(reader)

	return fromEncodeable(i, cs).Deref(cs)
}

func fromEncodeable(i interface{}, cs chunks.ChunkSource) Future {
	switch i := i.(type) {
	case bool:
		return futureFromValue(Bool(i))
	case int8:
		return futureFromValue(Int8(i))
	case int16:
		return futureFromValue(Int16(i))
	case int32:
		return futureFromValue(Int32(i))
	case int64:
		return futureFromValue(Int64(i))
	case float32:
		return futureFromValue(Float32(i))
	case float64:
		return futureFromValue(Float64(i))
	case uint8:
		return futureFromValue(UInt8(i))
	case uint16:
		return futureFromValue(UInt16(i))
	case uint32:
		return futureFromValue(UInt32(i))
	case uint64:
		return futureFromValue(UInt64(i))
	case string:
		return futureFromValue(NewString(i))
	case ref.Ref:
		return futureFromRef(i)
	case io.Reader:
		data, err := ioutil.ReadAll(i)
		d.Chk.NoError(err)
		return futureFromValue(newBlobLeaf(data))
	case []interface{}:
		return futureListFromIterable(i, cs)
	case enc.Map:
		return futureMapFromIterable(i, cs)
	case enc.Set:
		return futureSetFromIterable(i, cs)
	case enc.CompoundBlob:
		blobs := make([]Future, len(i.Blobs))
		for idx, blobRef := range i.Blobs {
			blobs[idx] = fromEncodeable(blobRef, cs)
		}
		cb := compoundBlob{i.Offsets, blobs, &ref.Ref{}, cs}
		return futureFromValue(cb)
	case enc.CompoundList:
		lists := make([]Future, len(i.Lists))
		for idx, listRef := range i.Lists {
			lists[idx] = fromEncodeable(listRef, cs)
		}
		cl := compoundList{i.Offsets, lists, &ref.Ref{}, cs}
		return futureFromValue(cl)
	default:
		d.Exp.Fail(fmt.Sprintf("Unknown encodeable", "%+v", i))
	}

	return nil
}

func futureListFromIterable(items []interface{}, cs chunks.ChunkSource) Future {
	output := futuresFromIterable(items, cs)
	return futureFromValue(listFromFutures(output, cs))
}

func futureMapFromIterable(items []interface{}, cs chunks.ChunkSource) Future {
	output := futuresFromIterable(items, cs)
	return futureFromValue(mapFromFutures(output, cs))
}

func futureSetFromIterable(items []interface{}, cs chunks.ChunkSource) Future {
	output := futuresFromIterable(items, cs)
	return futureFromValue(setFromFutures(output, cs))
}

func futuresFromIterable(items []interface{}, cs chunks.ChunkSource) (f []Future) {
	f = make([]Future, len(items))
	for i, inVal := range items {
		f[i] = fromEncodeable(inVal, cs)
	}
	return
}
