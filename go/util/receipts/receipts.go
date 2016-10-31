// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package receipts

import (
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"net/url"
	"time"

	"github.com/attic-labs/noms/go/d"
	"golang.org/x/crypto/nacl/secretbox"
)

// Data stores parsed receipt data.
type Data struct {
	Database  string
	IssueDate time.Time
}

// keySize is the size in bytes of receipt keys.
const keySize = 32 // secretbox uses 32-byte keys

// Key is used to encrypt receipt data.
type Key [keySize]byte

// nonceSize is the size in bytes that secretbox uses for nonces.
const nonceSize = 24

// DecodeKey converts a base64 encoded string to a receipt key.
func DecodeKey(s string) (key Key, err error) {
	var keySlice []byte
	keySlice, err = base64.URLEncoding.DecodeString(s)

	if err != nil {
		return
	}

	if len(keySlice) != len(key) {
		err = fmt.Errorf("--key must be %d bytes when decoded, not %d", len(key), len(keySlice))
		return
	}

	copy(key[:], keySlice)
	return
}

// Generate returns a receipt for Data, which is an encrypted query string
// encoded as base64.
func Generate(key Key, data Data) (string, error) {
	d.PanicIfTrue(data.Database == "" || data.IssueDate == (time.Time{}))

	receiptPlain := []byte(url.Values{
		"Database":  []string{hash(data.Database)},
		"IssueDate": []string{data.IssueDate.Format(time.RFC3339Nano)},
	}.Encode())

	var nonce [nonceSize]byte
	rand.Read(nonce[:])

	var keyBytes [keySize]byte = key
	receiptSealed := secretbox.Seal(nil, receiptPlain[:], &nonce, &keyBytes)

	// Put the nonce before the main receipt data.
	receiptFull := make([]byte, len(nonce)+len(receiptSealed))
	copy(receiptFull, nonce[:])
	copy(receiptFull[nonceSize:], receiptSealed)

	return base64.URLEncoding.EncodeToString(receiptFull), nil
}

// Verify verifies that a generated receipt grants access to a database. The
// IssueDate field will be populated from the decrypted receipt.
//
// Returns a tuple (ok, error) where ok is true if verification succeeds and
// false if not. Error is non-nil if the receipt itself is invalid.
func Verify(key Key, receiptText string, data *Data) (bool, error) {
	d.PanicIfTrue(data.Database == "")

	receiptSealed, err := base64.URLEncoding.DecodeString(receiptText)
	if err != nil {
		return false, err
	}

	minSize := nonceSize + secretbox.Overhead
	if len(receiptSealed) < minSize {
		return false, fmt.Errorf("Receipt is too short, must be at least %d bytes", minSize)
	}

	// The nonce is before the main receipt data.
	var nonce [nonceSize]byte
	copy(nonce[:], receiptSealed)

	var keyBytes [keySize]byte = key
	receiptPlain, ok := secretbox.Open(nil, receiptSealed[nonceSize:], &nonce, &keyBytes)
	if !ok {
		return false, fmt.Errorf("Failed to decrypt receipt")
	}

	query, err := url.ParseQuery(string(receiptPlain))
	if err != nil {
		return false, fmt.Errorf("Receipt is not a valid query string")
	}

	database := query.Get("Database")
	if database == "" {
		return false, fmt.Errorf("Receipt is missing a Database field")
	}

	dateString := query.Get("IssueDate")
	if dateString == "" {
		return false, fmt.Errorf("Receipt is missing an IssueDate field")
	}

	date, err := time.Parse(time.RFC3339Nano, dateString)
	if err != nil {
		return false, err
	}

	data.IssueDate = date
	return hash(data.Database) == database, nil
}

func hash(s string) string {
	h := sha512.Sum512_224([]byte(s))
	return base64.URLEncoding.EncodeToString(h[:])
}
