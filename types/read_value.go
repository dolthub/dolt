package types

import (
	"io"
	"io/ioutil"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/ref"
)

// ReadValue reads and decodes a value from a chunk source. It is not considered an error for the requested chunk to be absent from cs; in this case, the function simply returns nil, nil.
func ReadValue(r ref.Ref, cs chunks.ChunkSource) (Value, error) {
	dbg.Chk.NotNil(cs)
	reader, err := cs.Get(r)
	if reader != nil {
		defer reader.Close()
	}
	if reader == nil || err != nil {
		// Consider rejiggering this error handling with BUG 176.
		return nil, err // Get() will return nil, nil if chunk isn't present.
	}

	i, err := enc.Decode(reader)
	// Consider rejiggering this error handling with BUG 176.
	if err != nil {
		return nil, err
	}

	// Consider rejiggering this error handling with BUG 176.
	f, err := fromEncodeable(i, cs)
	if err != nil {
		return nil, err
	}

	// Consider rejiggering this error handling with BUG 176.
	val, err := f.Deref(cs)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func fromEncodeable(i interface{}, cs chunks.ChunkSource) (Future, error) {
	switch i := i.(type) {
	case bool:
		return futureFromValue(Bool(i)), nil
	case int16:
		return futureFromValue(Int16(i)), nil
	case int32:
		return futureFromValue(Int32(i)), nil
	case int64:
		return futureFromValue(Int64(i)), nil
	case float32:
		return futureFromValue(Float32(i)), nil
	case float64:
		return futureFromValue(Float64(i)), nil
	case uint16:
		return futureFromValue(UInt16(i)), nil
	case uint32:
		return futureFromValue(UInt32(i)), nil
	case uint64:
		return futureFromValue(UInt64(i)), nil
	case string:
		return futureFromValue(NewString(i)), nil
	case ref.Ref:
		return futureFromRef(i), nil
	case io.Reader:
		data, err := ioutil.ReadAll(i)
		dbg.Chk.NoError(err)
		return futureFromValue(newBlobLeaf(data)), nil
	case []interface{}:
		return futureListFromIterable(i, cs)
	case enc.Map:
		return futureMapFromIterable(i, cs)
	case enc.Set:
		return futureSetFromIterable(i, cs)
	case enc.CompoundBlob:
		blobs := make([]Future, len(i.Blobs))
		for idx, blobRef := range i.Blobs {
			f, err := fromEncodeable(blobRef, cs)
			if err != nil {
				return nil, err
			}
			blobs[idx] = f
		}
		cb := compoundBlob{i.Offsets, blobs, &ref.Ref{}, cs}
		return futureFromValue(cb), nil
	default:
		dbg.Chk.Fail("Unknown encodeable", "%+v", i)
	}
	return nil, nil
}

func futureListFromIterable(items []interface{}, cs chunks.ChunkSource) (Future, error) {
	output, err := futuresFromIterable(items, cs)
	if err != nil {
		return nil, err
	}
	return futureFromValue(listFromFutures(output, cs)), nil
}

func futureMapFromIterable(items []interface{}, cs chunks.ChunkSource) (Future, error) {
	output, err := futuresFromIterable(items, cs)
	if err != nil {
		return nil, err
	}
	return futureFromValue(mapFromFutures(output, cs)), nil
}

func futureSetFromIterable(items []interface{}, cs chunks.ChunkSource) (Future, error) {
	output, err := futuresFromIterable(items, cs)
	if err != nil {
		return nil, err
	}
	return futureFromValue(setFromFutures(output, cs)), nil
}

func futuresFromIterable(items []interface{}, cs chunks.ChunkSource) (f []Future, err error) {
	f = make([]Future, len(items))
	for i, inVal := range items {
		outVal, err := fromEncodeable(inVal, cs)
		if err != nil {
			return nil, err
		}
		f[i] = outVal
	}
	return
}

func MustReadValue(ref ref.Ref, cs chunks.ChunkSource) Value {
	val, err := ReadValue(ref, cs)
	dbg.Chk.NoError(err)
	return val
}
