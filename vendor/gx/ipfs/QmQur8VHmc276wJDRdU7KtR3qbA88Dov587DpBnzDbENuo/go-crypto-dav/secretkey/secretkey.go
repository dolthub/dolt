package secretkey

import (
	"bytes"
	"crypto/aes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"syscall"

	"gx/ipfs/QmaPHkZLbQQbvcyavn8q1GFHg6o6yeceyHFSJ3Pjf3p3TQ/go-crypto/scrypt"
	"gx/ipfs/QmaPHkZLbQQbvcyavn8q1GFHg6o6yeceyHFSJ3Pjf3p3TQ/go-crypto/ssh/terminal"

	"gx/ipfs/QmQur8VHmc276wJDRdU7KtR3qbA88Dov587DpBnzDbENuo/go-crypto-dav/encoding/base32"
)

type Key [32]byte

func New() *Key {
	var key Key
	if _, err := rand.Read(key[:]); err != nil {
		panic("rand.Read error: " + err.Error())
	}
	return &key
}

const EncryptedKeyLength = 32 + 4 + 1 // key+hash+pad

// Encrypt a secret key using BIP38-style encryption.
func Encrypt(key *Key, passphrase []byte) []byte {
	dst := make([]byte, EncryptedKeyLength)
	copy(dst[0:32], key[:])

	h := hashKey(key[:])
	copy(dst[32:36], h)

	k, err := scrypt.Key(passphrase, h, 1<<18, 8, 1, 64)
	if err != nil {
		panic(err)
	}
	runtime.GC()

	for i := 0; i < 32; i++ {
		dst[i] ^= k[i]
	}

	c1, _ := aes.NewCipher(k[32:48])
	c1.Encrypt(dst[0:16], dst[0:16])

	c2, _ := aes.NewCipher(k[48:64])
	c2.Encrypt(dst[16:32], dst[16:32])

	// add padding
	dst[36] = 0xff
	return dst
}

func Decrypt(src []byte, passphrase []byte) (*Key, bool) {
	var dst Key
	copy(dst[0:32], src[0:32])

	h := src[32:36]

	k, err := scrypt.Key(passphrase, h, 1<<18, 8, 1, 64)
	if err != nil {
		panic(err)
	}
	runtime.GC()

	c1, _ := aes.NewCipher(k[32:48])
	c1.Decrypt(dst[0:16], dst[0:16])

	c2, _ := aes.NewCipher(k[48:64])
	c2.Decrypt(dst[16:32], dst[16:32])

	for i := 0; i < 32; i++ {
		dst[i] ^= k[i]
	}

	hh := hashKey(dst[:])
	if !bytes.Equal(h, hh) {
		return nil, false
	}

	return &dst, true
}

func hashKey(k []byte) []byte {
	h1 := sha256.Sum256(k)
	h2 := sha256.Sum256(h1[:])
	return h2[0:4]
}

func WriteFile(key *Key, path string) error {
	passphrase, err := confirmPassphrase()
	if err != nil {
		return err
	}

	data := Encode(Encrypt(key, passphrase))
	return ioutil.WriteFile(path, data, 0600)
}

func ReadFile(path string) (*Key, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	ctxt, err := Decode(data)
	if err != nil {
		return nil, err
	}

	if len(ctxt) != EncryptedKeyLength {
		return nil, fmt.Errorf("invalid key size")
	}

	for {
		passphrase, err := securePrompt("Passphrase: ")
		if err != nil {
			return nil, fmt.Errorf("securePrompt: %s", err)
		}

		key, ok := Decrypt(ctxt, passphrase)
		if ok {
			return key, nil
		}

		fmt.Fprintln(os.Stderr, "Incorrect passphrase. Try again.")
	}
}

func Encode(data []byte) []byte {
	s := base32.EncodeToString(data)
	buf := new(bytes.Buffer)
	for i, r := range s {
		buf.WriteRune(r)
		if i > 0 && (i+1)%15 == 0 {
			buf.WriteByte('\n')
		} else if i > 0 && (i+1)%5 == 0 {
			buf.WriteByte(' ')
		}
	}
	return buf.Bytes()
}

func Decode(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, c := range data {
		if c == ' ' || c == '\n' {
			continue
		}
		buf.WriteByte(c)
	}
	x, err := base32.DecodeString(buf.String())
	if err != nil {
		return nil, fmt.Errorf("base32.DecodeString: %s", err)
	}
	return x, nil
}

func securePrompt(prompt string) ([]byte, error) {
	fmt.Fprint(os.Stderr, prompt)
	pw, err := terminal.ReadPassword(syscall.Stdin)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("terminal.ReadPassword error: %s", err)
	}
	return pw, nil
}

func confirmPassphrase() ([]byte, error) {
	for {
		passphrase, err := securePrompt("New Passphrase: ")
		if err != nil {
			return nil, fmt.Errorf("securePrompt: %s", err)
		}
		confirmation, err := securePrompt("Confirm Passphrase: ")
		if err != nil {
			return nil, fmt.Errorf("securePrompt: %s", err)
		}
		if bytes.Equal(passphrase, confirmation) {
			return passphrase, nil
		}
		fmt.Fprint(os.Stderr, "Passphrases do not match! Try again.\n\n")
	}
}
