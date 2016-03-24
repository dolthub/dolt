package datas

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"io"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

func serializeHints(w io.Writer, hints map[ref.Ref]struct{}) {
	err := binary.Write(w, binary.LittleEndian, uint32(len(hints))) // 4 billion hints is probably absurd. Maybe this should be smaller?
	d.Chk.NoError(err)
	for r := range hints {
		digest := r.Digest()
		n, err := io.Copy(w, bytes.NewReader(digest[:]))
		d.Chk.NoError(err)
		d.Chk.Equal(int64(sha1.Size), n)
	}
}

func deserializeHints(reader io.Reader) ref.RefSlice {
	numRefs := uint32(0)
	err := binary.Read(reader, binary.LittleEndian, &numRefs)
	d.Chk.NoError(err)

	refs := make(ref.RefSlice, numRefs)
	for i := uint32(0); i < numRefs; i++ {
		digest := ref.Sha1Digest{}
		n, err := io.ReadFull(reader, digest[:])
		d.Chk.NoError(err)
		d.Chk.Equal(int(sha1.Size), n)

		refs[i] = ref.New(digest)
	}
	return refs
}
