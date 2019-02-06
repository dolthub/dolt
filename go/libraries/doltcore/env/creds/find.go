package creds

import (
	"errors"
	"path/filepath"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
)

var ErrNotACred = errors.New("not a valid credential key id or public key")

func FindCreds(dEnv *env.DoltEnv, credsDir, pubKeyOrId string) (string, error) {
	if !B32CredsByteSet.ContainsAll([]byte(pubKeyOrId)) {
		return "", ErrBadB32CredsEncoding
	}

	if len(pubKeyOrId) == B32EncodedPubKeyLen {
		pubKeyOrId, _ = PubKeyStrToKIDStr(pubKeyOrId)
	}

	if len(pubKeyOrId) != B32EncodedKeyIdLen {
		return "", ErrNotACred
	}

	path := filepath.Join(credsDir, pubKeyOrId+JWKFileExtension)
	exists, isDir := dEnv.FS.Exists(path)

	if isDir {
		return path, filesys.ErrIsDir
	} else if !exists {
		return "", ErrCredsNotFound
	} else {
		return path, nil
	}
}
