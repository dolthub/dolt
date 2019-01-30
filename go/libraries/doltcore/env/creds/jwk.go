package creds

import (
	"encoding/base64"
	"encoding/json"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"io/ioutil"
	"path/filepath"
)

const (
	JWKFileExtension = ".jwk"
	ed25519Crv       = "Ed25519"
	kty              = "OKP"
)

type jwkData struct {
	D   *string `json:"d"`
	X   *string `json:"x"`
	Kty string  `json:"kty"`
	Crv string  `json:"crv"`
}

func JWKCredSerialize(creds DoltCreds) ([]byte, error) {
	if !creds.IsPubKeyValid() {
		panic("public key missing or invalid.  This is a bug.  Should be validated before calling")
	}

	pubKeyStr := base64.URLEncoding.EncodeToString(creds.PubKey)

	var privKeyStr string
	if creds.HasPrivKey() {
		if !creds.IsPrivKeyValid() {
			panic("Invalid private key. This is a bug. Should be validated before calling")
		}

		privKeyStr = base64.URLEncoding.EncodeToString(creds.PrivKey)
	}

	toSerialize := jwkData{&pubKeyStr, &privKeyStr, kty, ed25519Crv}
	data, err := json.Marshal(toSerialize)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func JWKCredsDeserialize(data []byte) (DoltCreds, error) {
	var jwk jwkData
	err := json.Unmarshal(data, &jwk)

	if err == nil {
		var pub, priv []byte
		pub, err = base64.URLEncoding.DecodeString(*jwk.D)

		if err == nil {
			if jwk.X != nil {
				priv, err = base64.URLEncoding.DecodeString(*jwk.X)
			}

			if err == nil {
				kid := PubKeyToKID(pub)
				return DoltCreds{pub, priv, kid}, nil
			}
		}
	}

	return DoltCreds{}, err
}

func JWKCredsWrite(wr io.Writer, creds DoltCreds) error {
	data, err := JWKCredSerialize(creds)

	if err != nil {
		return err
	}

	return iohelp.WriteAll(wr, data)
}

func JWKCredsRead(rd io.Reader) (DoltCreds, error) {
	data, err := ioutil.ReadAll(rd)

	if err != nil {
		return DoltCreds{}, err
	}

	return JWKCredsDeserialize(data)
}

func JWKCredsWriteToDir(fs filesys.Filesys, dir string, creds DoltCreds) (string, error) {
	outFile := filepath.Join(dir, creds.KeyID+JWKFileExtension)
	wr, err := fs.OpenForWrite(outFile)

	if err != nil {
		return "", err
	}

	func() {
		defer wr.Close()
		err = JWKCredsWrite(wr, creds)
	}()

	return outFile, err
}

func JWKCredsReadFromFile(fs filesys.Filesys, path string) (DoltCreds, error) {
	rd, err := fs.OpenForRead(path)

	if err == nil {
		defer rd.Close()
		return JWKCredsRead(rd)
	}

	return DoltCreds{}, err
}
