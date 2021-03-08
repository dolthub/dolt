package valuefile

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrCorruptNVF = errors.New("nvf file is corrupt")

func WritePrimitiveValueFile(ctx context.Context, filepath string, values ...types.Value) error {
	for _, v := range values {
		if !types.IsPrimitiveKind(v.Kind()) {
			return errors.New("non-primitve value found")
		}
	}

	nbf := types.Format_Default
	store, err := NewFileValueStore(nbf)

	if err != nil {
		return err
	}

	return WriteValueFile(ctx, filepath, store)
}

func WriteValueFile(ctx context.Context, filepath string, store *FileValueStore, values ...types.Value) (err error) {

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	if err != nil {
		return err
	}

	return WriteToWriter(ctx, f, store, values...)
}

func WriteToWriter(ctx context.Context, wr io.Writer, store *FileValueStore, values ...types.Value) error {
	db := datas.NewDatabase(store)
	ds, err := db.GetDataset(ctx, "master")

	if err != nil {
		return err
	}

	l, err := types.NewList(ctx, db, values...)

	if err != nil {
		return err
	}

	ds, err = db.CommitValue(ctx, ds, l)

	if err != nil {
		return err
	}

	ref, _, err := ds.MaybeHeadRef()

	if err != nil {
		return err
	}

	err = write(wr, ref.TargetHash(), store)

	if err != nil {
		return err
	}

	return nil
}

func write(wr io.Writer, h hash.Hash, store *FileValueStore) error {
	err := iohelp.WritePrimIfNoErr(wr, uint32(len(store.nbf.VersionString())), nil)
	err = iohelp.WriteIfNoErr(wr, []byte(store.nbf.VersionString()), err)
	err = iohelp.WriteIfNoErr(wr, h[:], err)
	err = iohelp.WritePrimIfNoErr(wr, uint32(store.numChunks()), err)

	if err != nil {
		return err
	}

	err = store.iterChunks(func(ch chunks.Chunk) error {
		h := ch.Hash()
		err = iohelp.WriteIfNoErr(wr, h[:], err)
		return iohelp.WritePrimIfNoErr(wr, uint32(len(ch.Data())), err)
	})

	err = store.iterChunks(func(ch chunks.Chunk) error {
		return iohelp.WriteIfNoErr(wr, ch.Data(), err)
	})

	return err
}

func ReadValueFile(ctx context.Context, filepath string) ([]types.Value, error) {
	f, err := os.Open(filepath)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	return ReadFromReader(ctx, f)
}

func ReadFromReader(ctx context.Context, rd io.Reader) ([]types.Value, error) {
	h, store, err := read(ctx, rd)

	if err != nil {
		return nil, err
	}

	db := datas.NewDatabase(store)
	v, err := db.ReadValue(ctx, h)

	if err != nil {
		return nil, err
	}

	commitSt, ok := v.(types.Struct)

	if !ok {
		return nil, ErrCorruptNVF
	}

	rootVal, ok, err := commitSt.MaybeGet(datas.ValueField)

	if err != nil {
		return nil, err
	}

	l := rootVal.(types.List)
	values := make([]types.Value, l.Len())
	err = l.IterAll(ctx, func(v types.Value, index uint64) error {
		values[index] = v
		return nil
	})

	if err != nil {
		return nil, err
	}

	return values, nil
}

func read(ctx context.Context, rd io.Reader) (hash.Hash, *FileValueStore, error) {
	errRd := iohelp.NewErrPreservingReader(rd)

	fmtLen, _ := errRd.ReadUint32(binary.BigEndian)
	data, err := iohelp.ReadNBytes(errRd, int(fmtLen))

	if err != nil {
		return hash.Hash{}, nil, err
	}

	var nbf *types.NomsBinFormat
	switch string(data) {
	case types.Format_7_18.VersionString():
		nbf = types.Format_7_18
	case types.Format_LD_1.VersionString():
		nbf = types.Format_LD_1
	default:
		return hash.Hash{}, nil, fmt.Errorf("unknown noms format: %s", string(data))
	}

	store, err := NewFileValueStore(nbf)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	hashBytes, _ := iohelp.ReadNBytes(errRd, hash.ByteLen)
	numChunks, err := errRd.ReadUint32(binary.BigEndian)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	type hashAndSize struct {
		h    hash.Hash
		size uint32
	}
	hashesAndSizes := make([]hashAndSize, numChunks)
	for i := uint32(0); i < numChunks; i++ {
		chHashBytes, _ := iohelp.ReadNBytes(errRd, hash.ByteLen)
		size, err := errRd.ReadUint32(binary.BigEndian)

		if err != nil {
			return hash.Hash{}, nil, err
		}

		hashesAndSizes[i] = hashAndSize{hash.New(chHashBytes), size}
	}

	for _, hashAndSize := range hashesAndSizes {
		h := hashAndSize.h
		size := hashAndSize.size
		chBytes, err := iohelp.ReadNBytes(errRd, int(size))

		if err != nil {
			return hash.Hash{}, nil, err
		}

		ch := chunks.NewChunk(chBytes)

		if h != ch.Hash() {
			return hash.Hash{}, nil, errors.New("data corrupted")
		}
		store.Put(ctx, ch)
	}

	return hash.New(hashBytes), store, nil
}
