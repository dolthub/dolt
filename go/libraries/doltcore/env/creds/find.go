package creds

import (
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"path/filepath"
)

var ErrNotACred = errors.New("not a valid credential key id or public key")

func FindCreds(dEnv *env.DoltEnv, credsDir, pubKeyOrId string) (string, error) {
	if !B32CredsByteSet.ContainsAll([]byte(pubKeyOrId)) {
		return "", ErrBadB32CredsEncoding
	}

	switch {
	case len(pubKeyOrId) == B32EncodedPubKeyLen:
		pubKeyOrId, _ = PubKeyStrToKIDStr(pubKeyOrId)
		fallthrough

	case len(pubKeyOrId) == B32EncodedKeyIdLen:
		path := filepath.Join(credsDir, pubKeyOrId+JWKFileExtension)
		exists, isDir := dEnv.FS.Exists(path)

		if isDir {
			return path, filesys.ErrIsDir
		} else if !exists {
			return "", ErrCredsNotFound
		} else {
			return path, nil
		}

	default:
		return "", ErrNotACred
	}
}
