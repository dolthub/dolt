package ldio

import (
	"encoding/binary"
	"io"
)

func WriteIfNoErr(w io.Writer, bytes []byte, err error) error {
	if err != nil {
		return err
	}

	return WriteAll(w, bytes)
}

func WritePrimIfNoErr(w io.Writer, prim interface{}, err error) error {
	if err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, prim)
}

func WriteAll(w io.Writer, data []byte) error {
	dataSize := len(data)
	for written := 0; written < dataSize; {
		n, err := w.Write(data[written:])

		if err != nil {
			return err
		}

		written += n
	}

	return nil
}
