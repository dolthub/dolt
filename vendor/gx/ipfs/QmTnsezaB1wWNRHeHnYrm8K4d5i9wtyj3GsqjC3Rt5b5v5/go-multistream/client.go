package multistream

import (
	"errors"
	"io"
)

var ErrNotSupported = errors.New("protocol not supported")

func SelectProtoOrFail(proto string, rwc io.ReadWriteCloser) error {
	err := handshake(rwc)
	if err != nil {
		return err
	}

	return trySelect(proto, rwc)
}

func SelectOneOf(protos []string, rwc io.ReadWriteCloser) (string, error) {
	err := handshake(rwc)
	if err != nil {
		return "", err
	}

	for _, p := range protos {
		err := trySelect(p, rwc)
		switch err {
		case nil:
			return p, nil
		case ErrNotSupported:
		default:
			return "", err
		}
	}
	return "", ErrNotSupported
}

func handshake(rwc io.ReadWriteCloser) error {
	tok, err := ReadNextToken(rwc)
	if err != nil {
		return err
	}

	if tok != ProtocolID {
		return errors.New("received mismatch in protocol id")
	}

	err = delimWrite(rwc, []byte(ProtocolID))
	if err != nil {
		return err
	}

	return nil
}

func trySelect(proto string, rwc io.ReadWriteCloser) error {
	err := delimWrite(rwc, []byte(proto))
	if err != nil {
		return err
	}

	tok, err := ReadNextToken(rwc)
	if err != nil {
		return err
	}

	switch tok {
	case proto:
		return nil
	case "na":
		return ErrNotSupported
	default:
		return errors.New("unrecognized response: " + tok)
	}
}
